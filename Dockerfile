# Image de base officielle Python slim (légère)
FROM python:3.11-slim

# Injection du binaire uv depuis l'image officielle Astral
# Plus rapide que pip install uv et ne pollue pas l'environnement Python
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Répertoire de travail dans le container
WORKDIR /app

# Copie du manifeste et du lockfile en premier (optimise le cache Docker)
# Si ces fichiers n'ont pas changé, Docker réutilise le layer d'installation
COPY pyproject.toml uv.lock ./

# uv sync installe exactement ce qui est dans uv.lock (--frozen = pas de résolution)
# Le venv est créé dans /app/.venv
RUN uv sync --frozen --no-dev

# Ajout du venv au PATH pour que `python` pointe sur le bon interpréteur
ENV PATH="/app/.venv/bin:$PATH"

# Copie du code ETL
COPY etl/ ./etl/

# Copie du dossier data (contient le fichier .xlsx)
COPY data/ ./data/

# Par défaut, le container n'exécute rien automatiquement.
# Chaque script ETL est appelé explicitement par Airflow via BashOperator.
CMD ["python", "-c", "print('ETL container ready.')"]
