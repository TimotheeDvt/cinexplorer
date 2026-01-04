# 🎬 CinéExplorer

**CinéExplorer** est une plateforme web de découverte de films développée avec **Django**. Le projet repose sur une architecture de données hybride (**SQL & NoSQL**) exploitant **SQLite** pour les données relationnelles et un **Replica Set MongoDB** pour les données orientées documents.

## 🚀 Fonctionnalités principales
* **Accueil Dynamique** : Statistiques en temps réel (nombre de films, acteurs, réalisateurs), Top 10 des films les mieux notés (via MongoDB) et découvertes aléatoires.
* **Répertoire complet** : Liste paginée des films avec filtres multicritères (genre, année, note minimale) et tris personnalisables (titre, année, note).
* **Recherche Groupée** : Moteur de recherche rapide interrogeant simultanément les titres de films et les noms de personnes via SQLite.
* **Fiches Détail Riches** : Informations exhaustives extraites de documents MongoDB structurés (casting complet avec personnages, réalisateurs, scénaristes et titres alternatifs).
* **Dashboard Statistiques** : Visualisations interactives via **Chart.js** (chargement asynchrone) présentant la distribution des notes, les films par genre et l'évolution par décennie.

## 🏗️ Architecture & Stratégie Multi-Bases
Le projet utilise une séparation stratégique des responsabilités pour maximiser les performances :
* **SQLite (Normalisé 3NF)** : Utilisé pour la recherche textuelle (`LIKE`), le filtrage complexe et la pagination rapide des listes.
* **MongoDB (Document Structuré)** : La collection `MOVIE_COMPLETE` stocke des documents dénormalisés, permettant de charger une fiche film complète et ses relations en une seule requête, réduisant la latence de lecture de plus de 70% par rapport au SQL.
* **Haute Disponibilité** : MongoDB est configuré en **Replica Set (3 nœuds)** pour garantir la tolérance aux pannes et la continuité de service avec un mécanisme d'élection automatique.

## 🛠️ Stack Technique
* **Backend** : Python 3.10+, Django 6.0.
* **Bases de données** : SQLite 3, MongoDB 8.2 (Replica Set).
* **Frontend** : Bootstrap 5 (Responsive Design), Chart.js (Visualisation), JavaScript (Chargement asynchrone / Skeletons).

## 📦 Installation & Lancement

1.  **Cloner le dépôt** :
    ```bash
    git clone [https://github.com/timotheedvt/cinexplorer.git](https://github.com/timotheedvt/cinexplorer.git)
    cd cinexplorer
    ```

2.  **Installer les dépendances** :
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configurer les bases de données** :
    * Placez le fichier `imdb.db` dans le dossier `data/`.
    * Lancez le **Replica Set MongoDB** local sur les ports 27017, 27018 et 27019.
    * Appliquez les migrations Django : `python manage.py migrate`.

4.  **Lancer le serveur** :
    ```bash
    python manage.py runserver
    ```

---
*Projet réalisé dans le cadre du module **Bases de Données Avancées (4A-BDA)** à Polytech Marseille par **Timothée DRAVET**.*