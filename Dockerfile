FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# HTTP-сервера у бота нет — это фоновый воркер. Если платформа (Coolify и т.п.)
# требует healthcheck по порту, отключи healthcheck в настройках приложения.
CMD ["python", "-m", "src.main"]
