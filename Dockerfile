# Use an official lightweight Python runtime
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all our Python application scripts into the container
COPY processor.py .
COPY api.py .
COPY simulator.py .

# Expose the port our FastAPI gateway runs on
EXPOSE 8000

# By default, this container will run our stream processor
CMD ["python", "processor.py"]