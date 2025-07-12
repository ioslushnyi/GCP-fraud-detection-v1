# Use Python 3.12 slim base image
FROM python:3.12-slim

# Install OS-level dependencies
RUN apt-get update && apt-get install -y build-essential gcc

# Set working directory
WORKDIR /app

# Copy local code to the container
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the entrypoint
ENTRYPOINT ["python", "generate_events.py"]
