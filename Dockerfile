# Use an official Python runtime as a base image
FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y curl build-essential && rm -rf /var/lib/apt/lists/*

# Install Poetry via the official installer
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH (Poetry is typically installed in /root/.local/bin for the root user)
ENV PATH="/root/.local/bin:$PATH"

# Copy Poetry configuration files first for caching benefits
COPY pyproject.toml poetry.lock ./

# Install Python dependencies using Poetry
RUN poetry install --no-interaction --with dev --no-root

# Copy the rest of the application code
COPY . .

# Expose necessary ports
EXPOSE 8332 27017 6379

# Set environment variable to avoid buffering (optional)
ENV PYTHONUNBUFFERED=1

# Command to run the application
CMD ["poetry", "run", "python", "main.py"]