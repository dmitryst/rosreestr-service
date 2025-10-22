# app.py
import logging
import asyncio
import json
import uuid
import os
import math
from pathlib import Path
from fastapi import FastAPI, HTTPException

# --- Конфигурация ---
# Директория для файлов-задач (внутренняя очередь)
QUEUE_DIR_PATH = os.getenv("QUEUE_DIR", "rosreestr_queue")
QUEUE_DIR = Path(QUEUE_DIR_PATH)

# Директория, куда скрипт-обработчик складывает РЕЗУЛЬТАТЫ (geojson)
OUTPUT_DIR_PATH = os.getenv("OUTPUT_DIR", "output")
OUTPUT_DIR = Path(OUTPUT_DIR_PATH)

# Таймаут ожидания результата от внешнего скрипта в секундах
REQUEST_TIMEOUT = 60
# ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Rosreestr Cadastral Service (Queue-based)",
    version="2.1.0"
)

# --- Вспомогательные функции, перенесенные из C# ---

def webmercator_to_wgs84(x, y):
    """Преобразует координаты из Web Mercator (EPSG:3857) в WGS 84 (EPSG:4326)."""
    earth_radius = 6378137.0
    lon = (x / earth_radius) * 180.0 / math.pi
    lat = (2.0 * math.atan(math.exp(y / earth_radius)) - math.pi / 2.0) * 180.0 / math.pi
    return lon, lat

def get_first_point_from_coordinates(coordinates):
    """Рекурсивно извлекает первую пару координат из структуры любой вложенности."""
    current_element = coordinates
    # Погружаемся вглубь массива, пока не дойдем до элемента с числами
    while isinstance(current_element, list) and current_element and isinstance(current_element[0], list):
        current_element = current_element[0]
    
    if isinstance(current_element, list) and len(current_element) >= 2:
        # Убеждаемся, что элементы - числа
        if isinstance(current_element[0], (int, float)) and isinstance(current_element[1], (int, float)):
            return current_element
    return None

# --- Жизненный цикл приложения ---

@app.on_event("startup")
def on_startup():
    # Создаем необходимые директории
    QUEUE_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "geojson").mkdir(parents=True, exist_ok=True)
    logger.info(f"Директория для очереди задач: {QUEUE_DIR.resolve()}")
    logger.info(f"Директория для результатов (geojson): {(OUTPUT_DIR / 'geojson').resolve()}")

# --- API эндпоинты ---

@app.get("/coordinates/{cadastral_number}",
         summary="Получить координаты по кадастровому номеру (через очередь)",
         response_description="Массив с широтой и долготой: [latitude, longitude]")
async def get_coordinates(cadastral_number: str):
    task_id = str(uuid.uuid4())
    task_file = QUEUE_DIR / f"{task_id}.task"
    result_file = QUEUE_DIR / f"{task_id}.result"

    safe_filename = cadastral_number.replace(":", "_") + ".geojson"
    geojson_file = OUTPUT_DIR / "geojson" / safe_filename
    
    logger.info(f"Создание задачи {task_id} для {cadastral_number}")

    try:
        # Создаем файл задачи
        with open(task_file, 'w', encoding='utf-8') as f:
            f.write(cadastral_number)
            
        # Ожидаем появления файла с результатом
        for _ in range(REQUEST_TIMEOUT):
            if result_file.exists() or geojson_file.exists():
                logger.info(f"Задача {task_id}: Обнаружен файл результата или geojson.")
                
                result_data = {}
                
                # Если geojson существует, он имеет приоритет
                if geojson_file.exists():
                    if result_file.exists():
                        # Если есть и ошибка, и geojson - логируем ошибку как warning
                        with open(result_file, 'r', encoding='utf-8') as f:
                            error_content = json.load(f).get('error', 'Неизвестная ошибка')
                        logger.warning(
                            f"Задача {task_id}: Обнаружена ошибка, но GeoJSON файл найден. "
                            f"Игнорируем ошибку и обрабатываем GeoJSON. Ошибка: {error_content}"
                        )

                    with open(geojson_file, 'r', encoding='utf-8') as f:
                        result_data = json.load(f)
                else: # geojson не найден, значит ошибка в .result файле критическая
                    with open(result_file, 'r', encoding='utf-8') as f:
                        result_data = json.load(f)
                    error_detail = result_data.get('error', 'Неизвестная критическая ошибка')
                    logger.error(f"Задача {task_id}: Произошла критическая ошибка, GeoJSON файл не найден: {error_detail}")
                    if "403" in str(error_detail) or "forbidden" in str(error_detail).lower():
                        raise HTTPException(status_code=403, detail=error_detail)
                    else:
                        raise HTTPException(status_code=500, detail=error_detail)

                # Обрабатываем данные из geojson
                geometry = result_data.get('geometry')
                if not geometry or 'coordinates' not in geometry:
                    raise HTTPException(status_code=404, detail="Координаты не найдены в геометрии")

                first_point_coords = get_first_point_from_coordinates(geometry['coordinates'])
                if not first_point_coords:
                    raise HTTPException(status_code=404, detail="Не удалось извлечь точку из координат")

                lon, lat = first_point_coords[0], first_point_coords[1]

                crs_name = result_data.get('crs', {}).get('properties', {}).get('name', '')
                logger.info(f"Задача {task_id}: Обнаружена система координат: '{crs_name}'")
                
                # Проверяем на наличие '3857' в строке, убрав возможные пробелы
                is_mercator = False
                if '3857' in crs_name:
                    is_mercator = True
                # Запасной вариант: если CRS не указан, проверяем значения координат
                # Вероятно из-за того что библиотека падает при формировании KML, она не успевает записать crs в geojson
                elif not crs_name and (abs(lon) > 180 or abs(lat) > 90):
                    logger.warning(f"Задача {task_id}: CRS не указан, но значения координат ({lon}, {lat}) выходят за пределы WGS84. Предполагается, что это EPSG:3857.")
                    is_mercator = True

                if is_mercator:
                    logger.info(f"Задача {task_id}: Выполняется конвертация из EPSG:3857 для {cadastral_number}.")
                    lon, lat = webmercator_to_wgs84(lon, lat)
                else:
                    logger.info(f"Задача {task_id}: Координаты для {cadastral_number} уже в WGS84 (или система не определена как EPSG:3857).")
                
                return [lat, lon]

            await asyncio.sleep(1)

        # Если цикл завершился, а результата нет — таймаут
        logger.error(f"Задача {task_id}: Таймаут. Файл результата или GeoJSON не был создан за {REQUEST_TIMEOUT} секунд.")
        raise HTTPException(status_code=504, detail="Таймаут шлюза: скрипт обработки координат не ответил вовремя.")

    finally:
        # Очищаем файлы
        if task_file.exists(): os.remove(task_file)
        if result_file.exists(): os.remove(result_file)

@app.get("/", summary="Health Check")
def health_check():
    return {"status": "ok"}
