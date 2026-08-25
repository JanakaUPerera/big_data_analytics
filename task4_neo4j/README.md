# Task 4 - Graph Database Engineering with Neo4j

This folder contains the Dockerized implementation for Task 4 of the Big Data Analytics Technologies coursework.

The project loads a 5,000-record subset of the SNAP Cit-Patents citation dataset into Neo4j. Each distinct patent is represented as a `Patent` node and each citation is represented as a directed `CITES` relationship. The Cypher analysis covers graph verification, degree centrality, connectivity, citation paths, graph density, and degree distributions.

## Project layout

```text
docker-compose.yml                 Neo4j and optional preprocessing services
data/cit-Patents.txt               SNAP patent citation source dataset
scripts/prepare_patents.py         Creates the 5,000-row import subset
import/patent_citations_5000.csv    CSV consumed by Neo4j LOAD CSV
queries/task4_analysis.cypher       Import, verification, and analysis queries
outputs/                            Saved analysis results
neo4j-data/                         Persistent Neo4j database files
logs/                               Neo4j logs mount
```

## Prerequisites

- Docker Desktop with the Docker Compose plugin
- Docker Desktop running
- The SNAP `cit-Patents.txt` file stored at `data/cit-Patents.txt`

The dataset is available from the [SNAP Cit-Patents page](https://snap.stanford.edu/data/cit-Patents.html). No Python or Neo4j installation is required on the host. The preprocessing script runs in the container image `python:3.12-slim`.

## Start Neo4j

Open PowerShell in this folder and start the database:

```powershell
cd task4_neo4j
docker compose up -d neo4j
```

Check the service status and logs:

```powershell
docker compose ps
docker compose logs neo4j
```

Neo4j is available at:

- Browser: http://localhost:7474
- Bolt: `bolt://localhost:7687`

Use these credentials when prompted:

```text
Username: neo4j
Password: coursework12345
```

The credentials are configured in `docker-compose.yml` through `NEO4J_AUTH`. Change them before using this setup for anything beyond coursework experimentation.

## Prepare the 5,000-record subset

Run the optional preprocessing service:

```powershell
docker compose --profile preparation run --rm patent-preprocessor
```

This reads `/data/cit-Patents.txt` inside the preprocessing container, skips blank and comment lines, and writes the first 5,000 valid citation pairs to `/import/patent_citations_5000.csv`. The output is bind-mounted to the local `import/` directory.

Confirm that the import file exists:

```powershell
Test-Path .\import\patent_citations_5000.csv
```

## Import and analyse the graph

Execute the complete Cypher script inside the Neo4j container. PowerShell passes the local script through standard input, so the query file does not need to be copied into the database container:

```powershell
Get-Content .\queries\task4_analysis.cypher | docker exec -i coursework-neo4j cypher-shell -u neo4j -p coursework12345
```

The script performs these operations in sequence:

1. Verifies that Neo4j can read the CSV from `file:///patent_citations_5000.csv`.
2. Creates the uniqueness constraint `patent_id_unique` on `Patent.id`.
3. Imports distinct `Patent` nodes with `LOAD CSV`, `UNWIND`, and `MERGE`.
4. Imports directed `CITES` relationships with `MATCH` and `MERGE`.
5. Runs node, relationship, degree, connectivity, sample path, density, and distribution queries.

The `MERGE` operations make repeated execution idempotent for the same import file: existing nodes and relationships are matched instead of duplicated. Query results are printed by `cypher-shell`; the Cypher script does not automatically export every result to `outputs/`.

For interactive execution, open http://localhost:7474, sign in, and paste sections of [queries/task4_analysis.cypher](queries/task4_analysis.cypher) into the Neo4j Browser.

## Expected results from the saved run

The included summary records the following results for the 5,000 citation subset:

- Patent nodes: 5,912
- `CITES` relationships: 5,000
- Average in-degree and out-degree: approximately 0.8457
- Maximum out-degree: 23
- Maximum in-degree: 4
- Graph density: approximately 0.0001430786
- Patents with both incoming and outgoing citations: 0
- Two-hop citation paths in this subset: none

See [outputs/task4_results_summary.txt](outputs/task4_results_summary.txt) for the recorded interpretation and [outputs/06_degree_distribution_out_degree.csv](outputs/06_degree_distribution_out_degree.csv) for the exported out-degree distribution.

## Stop and clean up

Stop the Neo4j container while retaining the local database, import file, and logs:

```powershell
docker compose down
```

To stop and remove the Compose containers and network:

```powershell
docker compose down --remove-orphans
```

The bind-mounted `neo4j-data/` directory preserves the database between starts. Do not delete it unless a fresh database is required. To reset the coursework database, stop Neo4j first, then remove the contents of `neo4j-data/` and start the service again.

## Troubleshooting

If Neo4j is still starting, wait until the logs report that the server is ready before running the Cypher script:

```powershell
docker compose logs -f neo4j
```

If `LOAD CSV` reports that the file is missing, confirm that `import/patent_citations_5000.csv` exists and that Neo4j is running with the Compose bind mount. If a rerun reports existing data, this is expected because the import uses a uniqueness constraint and `MERGE`.
