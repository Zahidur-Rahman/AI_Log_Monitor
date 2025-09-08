
# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /code

# Copy the requirements file into the container at /code
COPY ./requirements.txt /code/requirements.txt

# Install any needed packages
ENV UV_HTTP_TIMEOUT=300
RUN pip install uv \
 && uv pip install --system torch==2.2.2+cpu --index-url https://download.pytorch.org/whl/cpu \
 && uv pip install --system --no-cache-dir --upgrade -r /code/requirements.txt

# Copy the application code into the container
COPY ./main.py /code/main.py

# Run the application
CMD ["python", "main.py"]
