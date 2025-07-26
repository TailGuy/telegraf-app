# Start from a Python base image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Upgrade system packages to reduce vulnerabilities
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

# Install system dependencies if needed (e.g., for certain libraries)
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt requirements.txt

# Install Python dependencies
# Make sure 'docker' is listed in your requirements.txt
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY ./app /app

# Expose the port the app runs on (should match the uvicorn command)
EXPOSE 7090

# Command to run the application using Uvicorn
# Make sure the port here matches the EXPOSE directive and docker-compose port mapping
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7090", "--log-config", "logging.conf"]