import asyncio
from asyncio import TaskGroup
from pathlib import Path

import aiofiles
from httpx import AsyncClient, Client, HTTPError
from redis import Redis
from psycopg2 import connect as pg_connect
from dotenv import load_dotenv
from csv import writer
from io import BytesIO, StringIO
from os import getenv
from orjson import loads, dumps
import logging

logger = logging.getLogger('manifest')
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.setLevel(logging.INFO)

MANIFEST_CACHE_KEY = 'd2:manifest'
ROOT = Path(__file__).parent
DATA = ROOT / 'data'
JSON = DATA / 'json'
CSV = DATA / 'csv'
JSONL = DATA / 'jsonl'
PARQUET = DATA / 'parquet'

httpx_params = {
    'headers': {
        'x-api-key': getenv('D2_API_KEY'),
    },
    'base_url': 'https://www.bungie.net',
    'verify': False,
    'follow_redirects': True,
    'http2': True
}

redis_params = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'decode_responses': False,
}

pg_params = {
    'password': getenv('D2_DB_PASSWORD'),
}


class RedisClient:
    def __init__(self):
        self.redis = Redis(**redis_params)


redis_client = RedisClient()


# --------------------------------------------------- #
#               Utility Functions
# --------------------------------------------------- #

def create_dir(path_name: Path):
    if not path_name.exists():
        path_name.mkdir(parents=True, exist_ok=True)
        logger.info(f'Created directory {path_name}')
    else:
        logger.info(f'Directory {path_name} already exists')

def create_dirs():
    create_dir(DATA)
    create_dir(JSON)
    create_dir(JSONL)
    create_dir(PARQUET)
    create_dir(CSV)


def write_bytes(filename: str | Path, data: bytes) -> None:
    try:
        with open(filename, "wb") as f:
            f.write(data)
            logger.info(f'Wrote {filename} to disk: {len(data)} bytes')
    except OSError as e:
        logger.error(f'Failed to write {filename}: {e}')


def write_text(filename: str | Path, text: str) -> None:
    try:
        with open(filename, "w", encoding='utf-8') as f:
            f.write(text)
            logger.info(f'Wrote {filename} to disk: {len(text)} bytes')
    except OSError as e:
        logger.error(f'Failed to write {filename}: {e}')


def read_text(filename: str | Path) -> str:
    try:
        with open(filename, "r", encoding='utf-8') as f:
            data = f.read()
            logger.info(f'Read {filename}: {len(data)} bytes')
            return data
    except OSError as e:
        logger.error(f'Failed to read {filename}: {e}')


def read_bytes(filename: str | Path) -> bytes:
    try:
        with open(filename, "rb") as f:
            data = f.read()
            logger.info(f'Read {filename}: {len(data)} bytes')
            return data
    except OSError as e:
        logger.error(f'Failed to read {filename}: {e}')


# --------------------------------------------------- #
#               Utility Functions
# --------------------------------------------------- #

def get_manifest() -> bytes | None:
    try:
        with Client(**httpx_params) as sync_client:
            cache_value = redis_client.redis.get(MANIFEST_CACHE_KEY)
            if cache_value:
                logger.info("Cache found: %s bytes", len(cache_value))
                return cache_value
            response = sync_client.get('/Platform/Destiny2/Manifest')
            response.raise_for_status()
            content = response.content
            redis_client.redis.set(MANIFEST_CACHE_KEY, content, ex=3600)
            return content
    except HTTPError as e:
        logger.error(f'Failed to read manifest: {e}')


async def get_json_file(client: AsyncClient, url: str, filename: str) -> None:
    try:
        cache_value = redis_client.redis.get(url)
        if cache_value:
            logger.info("Cache found: %s bytes", len(cache_value))
            # write_bytes(JSON / F'{filename}.json', cache_value)
            return None
        logger.info("Cache miss: key=%s", MANIFEST_CACHE_KEY)
        response = await client.get(url)
        response.raise_for_status()
        logger.info("HTTP2 | GET | %s", url)
        content = response.content
        logger.info(f'Content of {url}: {len(content)} bytes')
        logger.info('Setting Cache Value: key=%s', MANIFEST_CACHE_KEY)
        redis_client.redis.set(url, content, ex=3600)

        write_bytes(JSON / F'{filename}.json', content)
        return None
    except HTTPError as e:
        logger.error(f'Failed to read {filename}: {e}')


async def get_all_json_files(url_list: list[tuple[str, str]]):
    async with AsyncClient(**httpx_params) as async_client:
        async with TaskGroup() as tsk:
            for filename, url in url_list:
                tsk.create_task(get_json_file(async_client, url, filename))


async def make_csv_file(filename: str | Path):
    try:
        async with aiofiles.open(CSV / f'{filename}.csv', 'w', newline='', encoding='utf-8') as csv_descriptor:
            with open(JSON / f'{filename}.json', 'r', encoding='utf-8') as file:
                content = file.read()
                buffer = StringIO()
                csv_writer = writer(buffer, quotechar='"')
                csv_writer.writerow(["id", "json"])
                json_content = loads(content)
                for key, value in json_content.items():
                    k = int(key)
                    v = dumps(value).decode("utf-8")
                    csv_writer.writerow([k, v])
            await csv_descriptor.write(buffer.getvalue())
        logger.info(f"Processed File: {filename}")
    except OSError as e:
        logger.error(f'Failed to write {filename}: {e}')


async def make_all_csv_files():
    file_paths = JSON.iterdir()
    async with TaskGroup() as tsk:
        for file_path in file_paths:
            base_path = file_path.name.split(".")[0]
            tsk.create_task(make_csv_file(base_path))


def populate_postgres():
    pass


async def main():
    create_dirs()
    manifest_content = get_manifest()
    write_bytes('manifest.json', manifest_content)
    manifest_dict = loads(manifest_content)
    jwcc = manifest_dict['Response']['jsonWorldComponentContentPaths']['en']
    await get_all_json_files(list(jwcc.items()))
    await make_all_csv_files()
    return None


if __name__ == '__main__':
    asyncio.run(main())
    logger.info(f'Process Complete')
