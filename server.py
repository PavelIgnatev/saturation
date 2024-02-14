import json
import aiohttp
from bs4 import BeautifulSoup
import logging
import asyncio
import random
import string
import requests
import time
from aiohttp import web
import aiohttp_jinja2
import jinja2
import os
import datetime
import signal

STORE_PATH = os.path.join(os.getcwd(), "store")

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler("server.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
)

logger.addHandler(file_handler)

errored_part_message = "If you haveTelegram, you can"

async def shutdown(signal, loop):
    logger.info(f"Получен сигнал завершения {signal.name}, остановка сервера")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()



def change_proxy(url):
    while True:
        try:
            response = requests.get(url + "&format=json")
            data = response.json()
            print(data)
            if data.get("status", "").lower() in ["err"]:
                raise Exception("Ошибка при смене прокси " + url)
            time.sleep(10)
            break
        except Exception:
            print("Ошибка при смене прокси. Повторный запрос... Ждем 5 секунд...")
            time.sleep(5)


def generate_random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for _ in range(length))


async def enrich_account_description(
    session, account_name, accounts, error_counts, proxy
):
    try:
        async with session.get(
            f"https://t.me/{account_name}",
            proxy=proxy,
            ssl=False,
        ) as response:
            html = await response.text()

        soup = BeautifulSoup(html, "html.parser")

        description_element = soup.select_one(".tgme_page_description")
        description = (
            description_element.get_text(strip=True) if description_element else None
        )

        if description and errored_part_message in description:
            if account_name not in error_counts:
                error_counts[account_name] = 1
            else:
                error_counts[account_name] += 1

            if error_counts[account_name] < 2:
                accounts.append(account_name)
                return errored_part_message

            return None

        return description

    except Exception:
        if account_name not in error_counts:
            error_counts[account_name] = 1
        else:
            error_counts[account_name] += 1

        if error_counts[account_name] < 2:
            accounts.append(account_name)
            return errored_part_message

        return None


async def process_account_batch(
    session, account_batch, data, accounts, error_counts, proxy
):
    tasks = []

    for account_name in account_batch:
        task = asyncio.create_task(
            enrich_account_description(
                session, account_name, accounts, error_counts, proxy
            )
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    for description, account_name in zip(results, account_batch):
        if description and errored_part_message in description:
            logger.error(f"Описание пользователя {account_name}: не получено, ошибка")
            continue

        data["accounts"][account_name]["description"] = description
        logger.info(f"Описание пользователя {account_name}: {description}")


async def main(data, proxy, change_url):
    try:
        accounts = list(data["accounts"].keys())
        error_counts = {}

        while len(accounts) > 0:
            async with aiohttp.ClientSession() as session:
                print(f"Остаточное количество юзернеймов: {len(accounts)}")

                change_proxy(
                    change_url,
                )

                usernames_batch = random.sample(list(accounts), min(300, len(accounts)))
                accounts = list(set(accounts) - set(usernames_batch))
                await process_account_batch(
                    session, usernames_batch, data, accounts, error_counts, proxy
                )   

        current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_path = os.path.join(STORE_PATH, f"data_{current_date}.json")
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
        
        logger.info("Скрипт успешно завершен.")
    except Exception as e:
        logger.error(f"Скрипт не завершен. {e}")


async def index(request):
    files = [f for f in os.listdir(STORE_PATH) if f != ".gitkeep"]
    return aiohttp_jinja2.render_template("index.html", request, {"files": files})


async def download_file(request):
    filename = request.match_info.get("filename")
    file_path = os.path.join(STORE_PATH, filename)
    return web.FileResponse(file_path, headers={"Content-Disposition": "attachment"})


async def saturation(request):
    try:
        reader = await request.multipart()
        data = {}
        while True:
            field = await reader.next()
            if field is None:
                break

            data[field.name] = await field.read(decode=True)
    except Exception as e:
        return web.Response(text=f"Ошибка при обработке формы: {e}", status=400)

    full_data = data.get("file", {})
    json_data = json.loads(full_data)

    proxy = data.get("proxy", "")
    change_url = data.get("change_url", "")

    proxy_str = proxy.decode('utf-8')
    change_url_str = change_url.decode('utf-8')

    print(f"FULL: {json_data['accounts'].keys()}")
    print(f"PROXY: {proxy_str}")
    print(f"CHANGE_URL: {change_url_str}")

    if proxy and change_url and full_data:
        asyncio.create_task(main(json_data, proxy_str, change_url_str))
        return web.Response(text="Задача взята в работу.")
    else:
        return web.Response(text="Невалидные параметры для старта задачи. Обязательные поля для form-data: file, proxy, change_url")


app = web.Application()
aiohttp_jinja2.setup(
    app, loader=jinja2.FileSystemLoader(os.path.join(os.getcwd(), "templates"))
)
app.add_routes(
    [
        web.get("/", index),
        web.get("/download/{filename}", download_file),
    ]
)

app.add_routes([web.post("/saturation", saturation)])

async def log_requests(app, handler):
    async def middleware(request):
        start_time = time.time()
        response = await handler(request)
        duration = time.time() - start_time
        logger.info(
            f"{request.method} {request.path} - {response.status} - {duration:.4f} s"
        )
        return response

    return middleware

app.middlewares.append(log_requests)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda signame=signame: asyncio.create_task(shutdown(signame, loop))
        )
    web.run_app(app)
