# worker.py (финальная версия)
import logging
import asyncio
import json
import time
import os
import subprocess
from pathlib import Path

# --- Конфигурация ---
QUEUE_DIR_PATH = os.getenv("QUEUE_DIR", "rosreestr_queue")
QUEUE_DIR = Path(QUEUE_DIR_PATH)
OUTPUT_DIR_PATH = os.getenv("OUTPUT_DIR", "output")
OUTPUT_DIR = Path(OUTPUT_DIR_PATH)
POLL_INTERVAL = 1
# ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - WORKER - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_task(task_file: Path):
    """Обрабатывает один файл задачи."""
    task_id = task_file.stem
    result_error_file = QUEUE_DIR / f"{task_id}.error"

    try:
        with open(task_file, 'r', encoding='utf-8') as f:
            cadastral_number = f.read().strip()
        logger.info(f"Обработка задачи {task_id} для КН: {cadastral_number}")

        safe_filename_base = cadastral_number.replace(":", "_")
        geojson_tmp_path = (OUTPUT_DIR / "geojson" / f"{safe_filename_base}.geojson.tmp")
        geojson_final_path = (OUTPUT_DIR / "geojson" / f"{safe_filename_base}.geojson")

        # --- Выполнение через subprocess БЕЗ --output ---
        # Библиотека сама создаст файл в папке ./output/geojson
        command = f'python -m rosreestr2coord -c {cadastral_number}'

        rosreestr_output_filename = geojson_tmp_path.parent / (safe_filename_base + ".geojson")
        
        result  = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')

        # Проверяем, создан ли geojson, даже если команда вернула ошибку (из-за KML)
        if not rosreestr_output_filename.exists():
            # Если файла нет, это настоящая ошибка
            error_message = result.stderr or "rosreestr2coord не создал geojson файл."
            raise Exception(error_message)
        
        # Атомарно переименовываем файл, давая сигнал, что он готов
        rosreestr_output_filename.rename(geojson_final_path)
        logger.info(f"Задача {task_id} успешно завершена. Результат в {geojson_final_path}")

    except Exception as e:
        logger.error(f"Ошибка при обработке задачи {task_id}: {e}")
        with open(result_error_file, 'w', encoding='utf-8') as f:
            json.dump({"error": str(e)}, f)
    finally:
        # Удаляем файл задачи, чтобы не обрабатывать его снова
        if task_file.exists():
            os.remove(task_file)

async def main():
    logger.info("Воркер запущен и слушает директорию...")
    processed_tasks = set()
    while True:
        try:
            task_files = [f for f in QUEUE_DIR.glob('*.task') if f.is_file()]
            for task_file in task_files:
                if task_file.name not in processed_tasks:
                    processed_tasks.add(task_file.name)
                    process_task(task_file)
        except Exception as e:
            logger.error(f"Критическая ошибка в главном цикле воркера: {e}")
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())

