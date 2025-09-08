
# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /code

# Copy the requirements file into the container at /code
COPY ./requirements.txt /code/requirements.txt

# Install any needed packages
RUN pip install uv && uv pip install --system --no-cache-dir --upgrade -r /code/requirements.txt

# Copy the application code into the container
COPY ./main.py /code/main.py

# Expose port 8000
EXPOSE 8000

# Run the FastAPI application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
