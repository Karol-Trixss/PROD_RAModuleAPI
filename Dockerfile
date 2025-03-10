FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    freetds-dev \
    freetds-bin \
    unixodbc-dev \
    tdsodbc \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Configure FreeTDS
RUN echo "[MSSQL]\n\
host = 10.10.1.4\n\
port = 1433\n\
tds version = 7.4" > /etc/freetds.conf

WORKDIR /app

# Copy requirements first
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make startup script executable
RUN chmod +x startup.sh

EXPOSE 8000

CMD ["./startup.sh"]
