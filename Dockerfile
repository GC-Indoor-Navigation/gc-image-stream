FROM python:3.12.10-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

COPY app ./app

RUN groupadd --system gc \
    && useradd --system --gid gc --home-dir /app gc \
    && mkdir -p /var/lib/gc-stream/storage \
    && chown -R gc:gc /app /var/lib/gc-stream

USER gc

EXPOSE 8000 50052

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=12 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/readiness', timeout=2).read()"]

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
