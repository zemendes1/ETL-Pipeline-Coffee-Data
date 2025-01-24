# Tech-Stack-Demonstration

## Overview
This project demonstrates a Python application that interacts with an S3 bucket and a PostgreSQL database. The application is containerized using Docker and integrates with Grafana for visualization purposes.

## Features
- Connects to a PostgreSQL database.
- Uploads and downloads files to/from MinIo S3.
- Uses Docker for containerization.
- Pre-commit hooks for code quality checks.

## Prerequisites
- Docker
- Docker Compose

## Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Build and run the application:**
   Use Docker Compose to build and run the application:
   ```bash
   docker compose build
   docker compose up
   ```

3. **Access the application:**
   The application will run and execute the `main.py` script, which includes downloading a file from S3.

## Usage
- The application connects to a PostgreSQL database running in a Docker container.
- It downloads a specified file from an S3 bucket and saves it locally.

## Demonstration

![Demo Image 1](images/docker.png)
*Figure 1: Docker containerization of all services.*

![Demo Image 2](images/dBeaver.png)
*Figure 2: Database inspection using dBeaver.*

![Demo Image 3](images/Grafana.png)
*Figure 3: Grafana monitoring dashboard displaying application metrics.*