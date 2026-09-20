# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).


Additonal findings: 

Task B - Inspection came out complete: All SRC components are where they need to be and function how they are supposed to be.
Task D - With a bit of back tracking, I found out that forgetting to remove the semi-colon next to the POSTGRE_PASSWORD had a resulting effect that gave me an error on this task. Requiring me to back-track to task C and make those necessary changes.Within the three fields "config.py, airflow.yml, and docker compose.yml",specifically in the Password section.

``Inspection of PostgreSQL Initilization:``
 Name   |       Owner       
---------+-------------------
 audit   | dss150p
 curated | dss150p
 public  | pg_database_owner
 staging | dss150p
(4 rows) 

 Schema  |       Name        | Type  |  Owner  
---------+-------------------+-------+---------
 curated | sales_order_lines | table | dss150p
(1 row)

Task E 
- Environment output = (Retrieved using:python -m src.cli validate-env)
PROJECT_ROOT= C:\repovscode\dss150p-lab03-starter-main
DB host/database= localhost dss150p
Configured source= data/source  
- Docker/Compose status = (healthy)
- Git log = Output:
b762b31 (HEAD -> goal1-reproducible-environment, origin/main, main) Small changes to Task C, noted in the READme and Finished Task D
83772aa Forgot to include the Case for inspection in task B status in readme
6abab64 TASK C succesfully removed all fields revealing password
1edd47d Created .env with gitignore, including .env.example
21554ef Testing commit
- Explanation: I personally believe that configuration should be kept seperate from code because it contains sensitive information that need to be kept secret and private, hence why we have exclusions like the ones placed in gitignore.

``Goal 1 (7.6) Acceptance tests``
- Fresh virtual environment installs from requirements.txt. = Works as intended with no error, given that I am currently using python (3.12.10). However, it was previously stated that (3.14) did give me problems due to early build issues.
- python -m src.cli validate-env succeeds locally = works and prints PROJECT_ROOT, DB host/database, and Configured source.
- docker compose run --rm pipeline python -m src.cli validate-env succeeds. = Responds successfully with output 
  [+] run 1/1
 ✔ Container dss150p-postgres Running                                                0.0s
Container dss150p-postgres Waiting 
Container dss150p-postgres Healthy 
Container dss150p-lab03-starter-main-pipeline-run-19a5fb6e3644 Creating 
Container dss150p-lab03-starter-main-pipeline-run-19a5fb6e3644 Created 
PROJECT_ROOT= /app
DB host/database= postgres dss150p
Configured source= data/source
- PostgreSQL starts healthy and contains the expected schemas. = Starts healthy and contains the schemas for curated and non-curated.
- .env is not tracked by Git. = Confirmed 
- Code is divided into modules with clear responsibilities = Accomplished

## Goal 2 
- TASK B (8.2) = Syntax construction was able to produce the required snapshots, with respect to 8.1 requirements. Running the whole line within the command line "python -c "from src.extract.files import extract_sources; print(extract_sources('test_001'))" To confirm it returning the needed snapshots and creating the folder in data. 
``Note for 8.2 - The bulk of the syntax construction was gotten from built-in intellicence, while the refinement was done with the help of AI (Gemini). To help ensure functionality and accuracy of paths of the code.``


