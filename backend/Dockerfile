FROM python:3.13
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
ARG TARGETPLATFORM
ENV SUPERCRONIC_VERSION=v0.2.40

RUN echo "Building for TARGETPLATFORM=${TARGETPLATFORM} TARGETARCH=${TARGETARCH}" \
  && case "${TARGETARCH}" in \
      amd64) BIN="supercronic-linux-amd64" ;; \
      arm64) BIN="supercronic-linux-arm64" ;; \
      arm)   BIN="supercronic-linux-arm" ;; \
      *) echo "Unsupported arch: ${TARGETARCH}" && exit 1 ;; \
    esac \
  && curl -fsSL "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/${BIN}" -o /usr/local/bin/supercronic \
  && chmod +x /usr/local/bin/supercronic

COPY alembic.ini .
COPY alembic ./alembic
COPY src ./src

RUN useradd app
USER app

CMD ["python", "src/main.py"]
