FROM python:3.13-slim

WORKDIR /app

COPY requirements/monitoring.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps/heartbeat_exporter /app/apps/heartbeat_exporter

ENV PYTHONPATH=/app/apps/heartbeat_exporter

CMD ["python", "-m", "apps.heartbeat_exporter.main"]
