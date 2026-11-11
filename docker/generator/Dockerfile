FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 TZ=UTC
WORKDIR /app
COPY generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generator/ .
CMD ["python", "-u", "main.py"]
