# Documentation d’installation d’Airflow 

Disponible ici:
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

# Procédure pas à pas
### Télécharger le docker-compose
- Créer le dossier airflow dans ``/var/docker-data01/data``
- Changer le propriétaire ``sudo chown admin.renault airflow``
- Se positionner dans le dossier  ``/var/docker-data01/data/airflow``
- Télécharger le docker compose via l’instruction ci-dessous

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.0/docker-compose.yaml'
```

Changer le propriétaire

### Création des sous dossiers

- Exécuter la commande de création des sous dossiers

```bash
mkdir -p ./dags ./logs ./plugins ./config
```

- Ajout variable env AIRFLOW_UID

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
Bien utiliser le double quote

### Installation docker compose
Cf Ruben

### Lancer l’installation

Commande suivante, depuis le répertoire d’installation airflow :
```bash
sudo docker compose run airflow-cli airflow config list
```
Dans le fichier docker-compose.yaml, passer le paramètre ci-dessous à false

``AIRFLOW__CORE__LOAD_EXAMPLES: 'false'``

Dans le fichier airflow.cfg, passer le paramètre ci-dessous à false

``load_examples = False``

Commande suivante, depuis le répertoire d’installation airflow

```bash
sudo docker compose up airflow-init
```
```bash
sudo docker compose up -d
```

### Lancer Airflow

``http://srvdev1:8080/``

# désinstallation de airflow

```bash
docker compose down --volumes --remove-orphans
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)  
docker rmi $(docker images -q)
docker volume rm $(docker volume ls -q)
docker network rm $(docker network ls -q)
docker system prune -a --volumes
```