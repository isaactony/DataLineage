FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install dbt
RUN pip install dbt-postgres==1.6.0

# Set working directory
WORKDIR /app

# Copy dbt project files
COPY . /app/

# Install dbt packages
RUN dbt deps

# Default command
CMD ["dbt", "run"]
