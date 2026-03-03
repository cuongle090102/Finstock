# TimescaleDB Consumer Service Dockerfile

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY services/timescale_consumer/requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/
COPY services/timescale_consumer/ /app/

# Set Python path
ENV PYTHONPATH=/app

# Run the service
CMD ["python", "-u", "main.py"]
