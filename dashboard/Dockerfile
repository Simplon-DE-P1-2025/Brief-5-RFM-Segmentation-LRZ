# ════════════════════════════════════════════════════════════════════
# dashboard/Dockerfile — Image uv-based pour le service Flask
# Build context = racine du projet (cf. docker-compose.yml).
# ════════════════════════════════════════════════════════════════════

FROM python:3.11-slim

# Installer uv (binaire copié depuis l'image officielle)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Dépendances système : libpq pour psycopg2, curl pour le healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Couche cache : installer les deps avant de copier le code source
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Code de l'application
COPY dashboard/ ./dashboard/

ENV PYTHONUNBUFFERED=1 \
    FLASK_APP=dashboard.app \
    PATH="/app/.venv/bin:$PATH"

EXPOSE 8501

# Gunicorn 2 workers : suffisant pour une démo locale
CMD ["uv", "run", "gunicorn", "-b", "0.0.0.0:8501", "-w", "2", \
     "--timeout", "60", "--access-logfile", "-", "dashboard.app:app"]
