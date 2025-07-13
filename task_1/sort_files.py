import asyncio
import argparse
import aiofiles
import aiofiles.os # Цей імпорт все ще потрібен для aiofiles.os.makedirs
import os # Тепер os імпортується для os.walk
import logging
from pathlib import Path

# Налаштування логування
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

async def read_folder(source_path: Path, output_path: Path):
    """
    Асинхронно читає всі файли у вихідній папці та її підпапках
    та запускає копіювання.
    """
    tasks = []
    try:
        # Використовуємо asyncio.to_thread для запуску синхронної os.walk в окремому потоці
        for root, dirs, files in await asyncio.to_thread(os.walk, source_path):
            for file in files:
                source_file_path = Path(root) / file
                tasks.append(asyncio.create_task(copy_file(source_file_path, output_path)))
        await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"Помилка під час читання папки {source_path}: {e}")

async def copy_file(source_file_path: Path, output_path: Path):
    """
    Асинхронно копіює файл у відповідну підпапку на основі його розширення.
    """
    try:
        file_extension = source_file_path.suffix.lstrip('.').lower()
        if not file_extension:
            destination_folder = output_path / "no_extension"
        else:
            destination_folder = output_path / file_extension

        await aiofiles.os.makedirs(destination_folder, exist_ok=True) # Використання aiofiles.os.makedirs
        destination_file_path = destination_folder / source_file_path.name

        async with aiofiles.open(source_file_path, 'rb') as src:
            async with aiofiles.open(destination_file_path, 'wb') as dest:
                while True:
                    chunk = await src.read(4096)
                    if not chunk:
                        break
                    await dest.write(chunk)
        logging.info(f"Файл '{source_file_path.name}' скопійовано до '{destination_folder}'")
    except Exception as e:
        logging.error(f"Помилка під час копіювання файлу '{source_file_path}': {e}")

async def main():
    parser = argparse.ArgumentParser(description="Асинхронне сортування файлів за розширенням.")
    parser.add_argument("--source", "-s", type=str, required=True, help="Шлях до вихідної папки.")
    parser.add_argument("--output", "-o", type=str, required=True, help="Шлях до цільової папки.")

    args = parser.parse_args()

    source_folder = Path(args.source)
    output_folder = Path(args.output)

    if not source_folder.is_dir():
        logging.error(f"Вихідна папка '{source_folder}' не існує або не є директорією.")
        return

    await aiofiles.os.makedirs(output_folder, exist_ok=True)

    logging.info(f"Запуск сортування файлів з '{source_folder}' до '{output_folder}'")
    await read_folder(source_folder, output_folder)
    logging.info("Сортування файлів завершено.")

if __name__ == "__main__":
    asyncio.run(main())
