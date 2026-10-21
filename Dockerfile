# 1. Base Image: A lightweight Linux distribution with Python 3.11 installed
FROM python:3.11-slim

# 2. Environment Variables:
# - PYTHONDONTWRITEBYTECODE=1: Prevents Python from writing .pyc files into the container
# - PYTHONUNBUFFERED=1: Flushes stdout/stderr immediately so logs appear in real-time
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. Working Directory: Set the default directory inside the container
WORKDIR /app

# 4. Install Dependencies First (Layer Caching Optimization):
# By copying only requirements.txt first, Docker caches the pip install layer.
# Docker won't reinstall packages on every build unless requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the Application Code:
# Copies common/, services/, place_order.py, etc. into /app inside the container
COPY . .
