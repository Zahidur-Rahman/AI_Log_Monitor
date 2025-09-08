FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
ENV UV_HTTP_TIMEOUT=300
RUN pip install uv \
 && uv pip install --system torch==2.2.2+cpu --index-url https://download.pytorch.org/whl/cpu \
 && uv pip install --system --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]