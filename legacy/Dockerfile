FROM python:3.11-slim

WORKDIR /app
COPY legacy/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY quotecore ./quotecore
COPY legacy ./legacy

EXPOSE 8000
CMD ["uvicorn", "legacy.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
