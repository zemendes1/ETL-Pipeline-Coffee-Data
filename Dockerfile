FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src ./src

# Install pre-commit hooks
RUN apt-get update && apt-get install -y git
COPY .pre-commit-config.yaml .
RUN git init && \
    pip install pre-commit && \
    pre-commit install

CMD ["python", "/app/src/main.py"]
