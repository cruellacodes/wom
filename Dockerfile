# Base image: Python 3.11 slim for smaller size and compatibility
FROM python:3.11-slim

# Set environment variables for cleaner logs and runtime behavior
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install system dependencies required for scientific libs and model backends
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app
ENV PYTHONPATH=/app/wom


# Copy application code into the container
COPY . .

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Expose port for uvicorn (used by Render or Docker runtime)
EXPOSE 10000

# Run the FastAPI app with uvicorn in production mode
CMD ["uvicorn", "wom.main:app", "--host", "0.0.0.0", "--port", "10000"]