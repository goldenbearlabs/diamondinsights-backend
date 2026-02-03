FROM python:3.13

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir --upgrade pip

COPY requirements/jobs.txt /tmp/requirements.txt
COPY requirements/base.txt /tmp/base.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps/jobs /app/apps/jobs
COPY shared /app/shared

ENV PYTHONPATH=/app/shared:/app/apps/jobs

CMD ["python", "-m", "apps.jobs.main"]