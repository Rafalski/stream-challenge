FROM python:3.11.18-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY pyproject.toml .
COPY src ./src
RUN pip install --no-cache-dir .
RUN useradd --create-home --uid 1000 appuser

USER appuser
CMD ["python", "-m", "stream_challenge.ingest"]
