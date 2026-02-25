# worker.py
import logging
import asyncio
import json
import time
import os
import subprocess
from pathlib import Path

# Данные прокси провайдера
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")
PROXY_USER = os.getenv("PROXY_USER")
PROXY_PASS = os.getenv("PROXY_PASS")

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

    # Файлы-сигнал
    result_error_file = QUEUE_DIR / f"{task_id}.error"
    result_notfound_file = QUEUE_DIR / f"{task_id}.not_found"
    result_forbidden_file = QUEUE_DIR / f"{task_id}.forbidden"

    try:
        with open(task_file, 'r', encoding='utf-8') as f:
            cadastral_number = f.read().strip()
        logger.info(f"Обработка задачи {task_id} для КН: {cadastral_number}")

        safe_filename_base = cadastral_number.replace(":", "_")
        geojson_tmp_path = (OUTPUT_DIR / "geojson" / f"{safe_filename_base}.geojson.tmp")
        geojson_final_path = (OUTPUT_DIR / "geojson" / f"{safe_filename_base}.geojson")

        # --- Настройка прокси ---
        if PROXY_HOST and PROXY_USER:
            # Используем строго http:// префикс для HTTP-прокси
            proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
            env = os.environ.copy()
            env["HTTP_PROXY"] = proxy_url
            env["HTTPS_PROXY"] = proxy_url
            logger.info(f"Используем HTTP-прокси: {PROXY_HOST}:{PROXY_PORT}")
        else:
            logger.warning("Переменные PROXY_... не найдены. Работаем без прокси.")
            env = os.environ.copy()

        # --- Выполнение через subprocess БЕЗ --output ---
        # Библиотека сама создаст файл в папке ./output/geojson
        command = f'python -m rosreestr2coord -c {cadastral_number}'

        rosreestr_output_filename = geojson_tmp_path.parent / (safe_filename_base + ".geojson")
        
        result  = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8', env=env)

        # Собираем весь выхлоп скрипта в одну переменную для отладки
        full_log = f"\n--- STDOUT ---\n{result.stdout}\n--- STDERR ---\n{result.stderr}\n----------------"

        # Объединяем stdout и stderr для поиска текста ошибки
        output_text = (result.stdout + result.stderr).lower()

        if "403" in output_text and "forbidden" in output_text:
            logger.warning(f"Задача {task_id}: Для КН {cadastral_number} доступ запрещен (403). Создаем сигнал .forbidden")
            result_forbidden_file.touch()
            return

        if "nothing found" in output_text.lower():
            logger.warning(f"Задача {task_id}: Для КН {cadastral_number} ничего не найдено. Создаем сигнал .not_found")
            result_notfound_file.touch() # Создаем пустой файл-сигнал
            return

        # Проверяем, создан ли geojson, даже если команда вернула ошибку (из-за KML)
        if not rosreestr_output_filename.exists():
            # Если файла нет, это настоящая ошибка
            logger.error(f"Задача {task_id}: Файл не создан. Полный ответ утилиты: {full_log}")

            error_message = result.stderr.strip() or result.stdout.strip() or "rosreestr2coord не создал geojson файл."
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

