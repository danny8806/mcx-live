# Root image: LIVE gateway (real-broker), port 8001 (same as services/live-mcx).
# Multi-stage so the frontend is built inside Docker (dashboard-ui/dist baked in).
FROM node:20-alpine AS frontend-build
WORKDIR /ui
COPY dashboard-ui/package.json dashboard-ui/package-lock.json ./
RUN npm ci
COPY dashboard-ui/ ./
RUN chmod -R +x node_modules && npm run build

FROM python:3.11-slim AS final
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=frontend-build /ui/dist ./dashboard-ui/dist

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=5 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8001/health',timeout=3)" || exit 1

CMD ["python", "-m", "live.run"]