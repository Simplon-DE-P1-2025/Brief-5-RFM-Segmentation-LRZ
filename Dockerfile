# Image de base officielle Python slim (légère)
FROM python:3.11-slim

# Injection du binaire uv depuis l'image officielle Astral
# Plus rapide que pip install uv et ne pollue pas l'environnement Python
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Création d'un utilisateur non-root (bonne pratique de sécurité)
# Un container qui tourne en root peut compromettre l'hôte en cas de faille
RUN groupadd --gid 1001 etlgroup && \
    useradd --uid 1001 --gid etlgroup --no-create-home etluser

# Répertoire de travail dans le container
WORKDIR /app

# Copie du manifeste et du lockfile en premier (optimise le cache Docker)
# Si ces fichiers n'ont pas changé, Docker réutilise le layer d'installation
COPY pyproject.toml uv.lock ./

# uv sync installe exactement ce qui est dans uv.lock (--frozen = pas de résolution)
# Le venv est créé dans /app/.venv — on reste root pour l'installation
RUN uv sync --frozen --no-dev

# Ajout du venv au PATH pour que `python` pointe sur le bon interpréteur
ENV PATH="/app/.venv/bin:$PATH"

# Copie du code ETL
COPY etl/ ./etl/

# Copie du dossier data (contient le fichier .xlsx)
COPY data/ ./data/

# Transfert de la propriété des fichiers à etluser, puis bascule vers lui
RUN chown -R etluser:etlgroup /app
USER etluser

# Par défaut, le container n'exécute rien automatiquement.
# Chaque script ETL est appelé explicitement par Airflow via BashOperator.
CMD ["python", "-c", "print('ETL container ready.')"]
