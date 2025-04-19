# reporting/Dockerfile
FROM python:3.9-slim

# Create a working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY app.py .

# Expose port 5010 now
EXPOSE 5010

# Default command to run the Flask app
CMD ["python", "app.py"]
