



### 1. Install Requirements
- Install [Docker Desktop](https://www.docker.com/products/docker-desktop).
- Install [Git](https://git-scm.com/downloads).

### 2. Clone the Repository
```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd final_project
### 3. Start Services
Run the containers:
docker-compose up -d
Postgres (database) → localhost:5432
• 	pgAdmin (database UI) → available at localhost:8081
• 	Login: admin@admin.com / admin
• 	Airflow Webserver → available at locahost:8080
• 	Login: airflow / airflow
### 3. Start Services
•	  Place your DAGs in the  folder.
• 	Place your ETL script in .
• 	Place your dataset () in the same folder.
• 	Trigger the DAG from the Airflow UI.




