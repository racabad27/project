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


## Additonal findings: 

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
- TASK A (8.2) = Syntax construction was able to produce the required snapshots, with respect to 8.1 requirements. Running the whole line within the command line "python -c "from src.extract.files import extract_sources; print(extract_sources('test_001'))" To confirm it returning the needed snapshots and creating the folder in data. 
``Note for 8.2 - The bulk of the syntax construction was gotten from built-in intellicence, while the refinement was done with the help of AI (Gemini). To help ensure functionality and accuracy of paths of the code.``

- Task B (8.3) = There were some initial problems that were presented upon initially running **"python -c "from src.extract.files import extract_sources; from src.transform.staging import stage_all; extract_sources('test_001'); print(stage_all('test_001'))"**. This was carefully inspected and troubleshooted using ``Gemini AI``. There was an initial debugging stage, first running a syntax that displays the column names of each file (customer.csv,orders.csv,products.json). It was revealed that "name" was not found in the the original customers.csv. Another was order_date was looked for instead of order_timestamp. Lastly, price in products.parquet was attempted to be located when it should have been unit_price. All-in-all, the bug was more on naming issues for specific columns. The second run fixed all these issues and successfully accomplished the task.

- Task C (8.4) = This was more carefully done, after learning from the previous tasks mistakes. The code was first structured into a functioning/workable state, where I asked assistance from ``"Gemini"`` AI to help refine and possible troubleshoot the code, if any issues or inefficiencies lie. It came back as positive, not reporting any issues, suggesting that the code was alright to be run via vscode terminal using "python -c "from src.extract.files import extract_sources; from src.transform.staging import stage_all; from src.transform.curated import curate_all; extract_sources('test_001'); stage_all('test_001'); print(curate_all('test_001'))". 

- Task D (8.5) = 
## Pipeline Execution and Orchestration

The pipeline is managed through a central command-line interface entrypoint located at `src/cli.py`. This script provides a modular execution interface that allows running individual pipeline phases independently or orchestrating the complete end-to-end flow in sequence. Every execution generates a unique, timestamped `run_id` to ensure isolated, reproducible runs and maintain audit lineage across all data layers.

### Purpose that it serves

- **Environment Validation (`validate-env`):** Inspects the active environment configuration, printing the project root path, database target details, and configured source data directory. Run this command first to verify settings before executing data operations.
- **Raw Extraction (`extract`):** Reads source datasets from the configured input directory and creates an immutable raw snapshot partitioned under `data/raw/run_id=<run_id>/`.
- **Data Transformation (`transform`):** Runs both staging and curated processing layers in sequence. The staging layer cleans fields, enforces schemas, deduplicates records, and routes invalid rows to quarantine. The curated layer joins staged entities, computes financial metrics (`gross_amount`, `discount_amount`, `net_amount`), generates record hashes, and flags orphan records.
- **End-to-End Execution (`run-all`):** Orchestrates the full ETL workflow (`extract` followed by `transform`), executing the pipeline end-to-end within a single execution context.

### Execution Examples
Through this we will be able to proceed with the following commands needed in (8.6)
- python -m src.cli run-all
- python -m src.cli load
- python -m src.cli load

