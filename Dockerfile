FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN dos2unix /docker-entrypoint.sh && chmod +x /docker-entrypoint.sh

COPY . .

EXPOSE 8000

ENTRYPOINT ["/bin/sh", "/docker-entrypoint.sh"]
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]

