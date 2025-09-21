FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY ./requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the application code
COPY ./app.py /app/app.py

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]