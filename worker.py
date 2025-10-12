# worker.py (финальная версия)
import logging
import json
import time
import os
import subprocess
from pathlib import Path

# --- Конфигурация ---
SHARED_DIR = Path("./rosreestr_queue")
# Папка, куда rosreestr2coord КЛАДЕТ РЕЗУЛЬТАТЫ по умолчанию
OUTPUT_DIR = Path("./output/geojson")
POLL_INTERVAL = 1
# ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - WORKER - %(levelname)s - %(message)s')

def process_task(task_file: Path):
    """Обрабатывает один файл задачи."""
    task_id = task_file.stem
    result_file = SHARED_DIR / f"{task_id}.result"
    
    # Имя файла, которое создаст библиотека
    cadastral_number_safe = ""

    try:
        with open(task_file, 'r', encoding='utf-8') as f:
            cadastral_number = f.read().strip()
        
        if not cadastral_number:
            raise ValueError("Файл задачи пуст.")
            
        logging.info(f"Обработка задачи {task_id} для КН: {cadastral_number}")

        # --- Выполнение через subprocess БЕЗ --output ---
        # Библиотека сама создаст файл в папке ./output/geojson
        command = f'python -m rosreestr2coord -c {cadastral_number}'
        
        proc = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')

        if proc.returncode != 0:
            raise RuntimeError(f"Ошибка rosreestr2coord: {proc.stderr.strip()}")

        # Формируем путь к файлу, который ДОЛЖНА была создать библиотека
        cadastral_number_safe = cadastral_number.replace(":", "_")
        expected_file = OUTPUT_DIR / f"{cadastral_number_safe}.geojson"
        
        if not expected_file.exists():
            raise FileNotFoundError(f"rosreestr2coord не создал ожидаемый файл: {expected_file}")
        
        logging.info(f"Найден файл результата: {expected_file}")
        
        # Читаем geojson из папки /output/geojson
        with open(expected_file, 'r', encoding='utf-8') as f_temp:
            geojson_data = json.load(f_temp)
        # ---

        # Запись успешного результата в .result файл для FastAPI
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(geojson_data, f)
        logging.info(f"Задача {task_id} успешно завершена")

    except Exception as e:
        logging.error(f"Ошибка при обработке задачи {task_id}: {e}")
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump({"error": str(e)}, f)
    finally:
        # Очистка
        if task_file.exists(): os.remove(task_file)
        # Очищаем и созданный библиотекой файл
        if cadastral_number_safe:
             final_output_file = OUTPUT_DIR / f"{cadastral_number_safe}.geojson"
             if final_output_file.exists(): os.remove(final_output_file)


def main_loop():
    """Главный цикл воркера."""
    logging.info(f"Воркер запущен. Ожидание задач в папке: {SHARED_DIR.resolve()}")
    SHARED_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True) # Создаем папку для результатов
    
    while True:
        try:
            task_files = list(SHARED_DIR.glob("*.task"))
            if task_files:
                process_task(task_files[0])
            else:
                time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            logging.info("Воркер остановлен.")
            break
        except Exception as e:
            logging.error(f"Неожиданная ошибка в главном цикле: {e}")
            time.sleep(POLL_INTERVAL * 5)

if __name__ == "__main__":
    main_loop()

