FROM python:3.13
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY alembic.ini .
COPY alembic ./alembic

COPY src ./src
EXPOSE 8080

RUN useradd app
USER app

CMD ["python", "src/main.py"]
