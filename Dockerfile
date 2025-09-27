# Use Python 3.10 slim image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY stream/ ./stream/
COPY intake/ ./intake/
COPY start.sh .

# Create non-root user and make start script executable
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app \
    && chmod +x /app/start.sh
USER app

# Expose port (Cloud Run will set PORT environment variable)
EXPOSE 8080

# Set environment variables
ENV PYTHONPATH=/app

# Run the application using the startup script
CMD ["./start.sh"]
