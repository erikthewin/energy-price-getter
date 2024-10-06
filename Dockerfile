# Use the official Python image from the Docker Hub
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy your script and any other necessary files into the container
COPY app.py .

# Install required packages
RUN pip install mysql-connector-python requests python-dotenv

# Command to run your script
CMD ["python", "app.py"]