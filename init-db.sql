-- Création de l'utilisateur et de la base de données métier RFM
-- Ce script est exécuté automatiquement par PostgreSQL au premier démarrage

CREATE USER rfm_user WITH PASSWORD 'rfm_pass';
CREATE DATABASE rfm_db OWNER rfm_user;
GRANT ALL PRIVILEGES ON DATABASE rfm_db TO rfm_user;
