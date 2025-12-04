# Use an official slim Python image
FROM --platform=linux/amd64 python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install required system packages and the Microsoft ODBC driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    build-essential \
    unixodbc \
    unixodbc-dev \
    gcc \
    g++ \
 && rm -rf /var/lib/apt/lists/*

# Add Microsoft package repository and install msodbcsql18 (accept EULA)
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
 && curl -sSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list -o /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
 && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy app code
COPY . /app

# Expose port and run with gunicorn
ENV PORT=5000
EXPOSE 5000

CMD ["gunicorn", "server:app", "--bind", "0.0.0.0:5000", "--workers", "2"]
