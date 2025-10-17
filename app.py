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
SHARED_DIR = Path("/app/rosreestr_queue") # Используем абсолютный путь для надежности в контейнере
REQUEST_TIMEOUT = 60
# ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Rosreestr Cadastral Service (Queue-based)",
    version="2.1.0"
)

# --- Вспомогательные функции, перенесенные из C# ---

def web_mercator_to_wgs84(x, y):
    """Преобразует координаты из Web Mercator (EPSG:3857) в WGS 84 (EPSG:4326)."""
    earth_radius = 6378137.0
    lon = (x / earth_radius) * 180.0 / math.pi
    lat = (2.0 * math.atan(math.exp(y / earth_radius)) - math.pi / 2.0) * 180.0 / math.pi
    return lon, lat

def get_first_point(coordinates):
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

# --- Основная логика ---

@app.on_event("startup")
def on_startup():
    SHARED_DIR.mkdir(exist_ok=True)
    logger.info(f"Общая папка для очереди: {SHARED_DIR.resolve()}")

@app.get("/coordinates/{cadastral_number}",
         summary="Получить координаты по кадастровому номеру (через очередь)",
         response_description="Массив с широтой и долготой: [latitude, longitude]")
async def get_coordinates(cadastral_number: str):
    task_id = str(uuid.uuid4())
    task_file = SHARED_DIR / f"{task_id}.task"
    result_file = SHARED_DIR / f"{task_id}.result"
    
    logger.info(f"Создание задачи {task_id} для {cadastral_number}")

    try:
        # 1. Создаем файл задачи
        with open(task_file, 'w', encoding='utf-8') as f:
            f.write(cadastral_number)
            
        # 2. Ожидаем появления файла с результатом
        for _ in range(REQUEST_TIMEOUT):
            if result_file.exists():
                logger.info(f"Результат для задачи {task_id} найден")
                with open(result_file, 'r', encoding='utf-8') as f:
                    result_data = json.load(f)

                if "error" in result_data:
                    logger.error(f"Воркер сообщил об ошибке: {result_data['error']}")
                    raise HTTPException(status_code=500, detail=f"Ошибка в воркере: {result_data['error']}")
                
                geometry = result_data.get("geometry")
                if not geometry or "coordinates" not in geometry:
                    raise HTTPException(status_code=404, detail="Геометрия или координаты отсутствуют в ответе.")
                
                first_point_coords = get_first_point(geometry["coordinates"])

                if not first_point_coords:
                    raise HTTPException(status_code=404, detail="Не удалось извлечь координаты из геометрии.")

                lon, lat = first_point_coords[0], first_point_coords[1]
                
                # Проверяем, нужна ли конвертация из EPSG:3857
                crs_name = result_data.get("crs", {}).get("properties", {}).get("name", "")
                if "EPSG:3857" in crs_name:
                    logger.info(f"Координаты для {cadastral_number} будут конвертированы из EPSG:3857.")
                    lon, lat = web_mercator_to_wgs84(lon, lat)
                    return [lat, lon] # Возвращаем в порядке [lat, lon]
                else:
                    # Если CRS не указан или это WGS84, то rosreestr2coord возвращает [lon, lat]
                    logger.info(f"Координаты для {cadastral_number} считаются WGS84. Конвертация не требуется.")
                    return [lat, lon] # Меняем местами и возвращаем [lat, lon]

            await asyncio.sleep(1)

        logger.error(f"Таймаут для задачи {task_id}. Файл результата не появился.")
        raise HTTPException(status_code=504, detail="Время ожидания ответа от воркера истекло.")

    finally:
        # 4. Очищаем файлы
        if task_file.exists(): os.remove(task_file)
        if result_file.exists(): os.remove(result_file)

@app.get("/", summary="Health Check")
def health_check():
    return {"status": "ok"}
