# app.py
import logging
import asyncio
import json
import uuid
import os
from pathlib import Path
from fastapi import FastAPI, HTTPException

# --- Конфигурация ---
# Общая папка для задач и результатов
SHARED_DIR = Path("./rosreestr_queue")
# Максимальное время ожидания результата (в секундах)
REQUEST_TIMEOUT = 60
# ---

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Rosreestr Cadastral Service (Queue-based)",
    version="2.0.0"
)

@app.on_event("startup")
def on_startup():
    # Создаем папку при старте, если ее нет
    SHARED_DIR.mkdir(exist_ok=True)
    logger.info(f"Общая папка для очереди: {SHARED_DIR.resolve()}")

@app.get("/coordinates/{cadastral_number}",
         summary="Получить координаты по кадастровому номеру (через очередь)",
         response_description="Массив с долготой и широтой: [longitude, latitude]")
async def get_coordinates(cadastral_number: str):
    task_id = str(uuid.uuid4())
    task_file = SHARED_DIR / f"{task_id}.task"
    result_file = SHARED_DIR / f"{task_id}.result"
    
    logger.info(f"Создание задачи {task_id} для {cadastral_number}")

    try:
        # 1. Создаем файл задачи с кадастровым номером
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
                
                coordinates = result_data.get("geometry", {}).get("coordinates")
                if not coordinates or not isinstance(coordinates, list) or not coordinates[0]:
                    raise HTTPException(status_code=404, detail="Координаты не найдены в ответе от воркера.")
                
                first_point = coordinates[0][0]
                return first_point
            
            await asyncio.sleep(1) # Проверяем раз в секунду

        # 3. Если цикл завершился — таймаут
        logger.error(f"Таймаут для задачи {task_id}. Файл результата не появился.")
        raise HTTPException(status_code=504, detail="Время ожидания ответа от воркера истекло.")

    finally:
        # 4. Очищаем файлы в любом случае
        if task_file.exists():
            os.remove(task_file)
        if result_file.exists():
            os.remove(result_file)

@app.get("/", summary="Health Check")
def health_check():
    return {"status": "ok"}
