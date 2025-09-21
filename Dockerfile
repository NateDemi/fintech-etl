# ---- Base ----
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY intake/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- App code ----
COPY intake/main.py .

# Cloud Run provides $PORT; default to 8080 locally
ENV PORT=8080

# ---- Run server ----
# Note: don't hardcode --port; respect $PORT from Cloud Run
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT}