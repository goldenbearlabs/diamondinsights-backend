FROM python:3.13
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ADD https://github.com/aptible/supercronic/releases/download/v0.2.29/supercronic-linux-amd64 /usr/local/bin/supercronic
RUN chmod +x /usr/local/bin/supercronic

COPY alembic.ini .
COPY alembic ./alembic
COPY src ./src

RUN useradd app
USER app

CMD ["python", "src/main.py"]
