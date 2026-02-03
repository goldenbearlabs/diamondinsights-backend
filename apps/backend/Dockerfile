FROM python:3.13
WORKDIR /app
ENV PYTHONPATH=/app

RUN pip install --no-cache-dir --upgrade pip

COPY requirements/backend.txt /tmp/requirements.txt
COPY requirements/base.txt /tmp/base.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps/backend/src /app/src
COPY apps/backend/alembic /app/alembic
COPY apps/backend/alembic.ini /app/alembic.ini
COPY shared /app/shared

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
