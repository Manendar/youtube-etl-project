# YouTube Data Engineering Pipeline

An end-to-end data engineering project that automatically downloads YouTube trending video data from Kaggle, processes it through a multi-zone HDFS data lake, runs analytics with PySpark, stores results in PostgreSQL, and visualizes insights in Metabase all orchestrated by Apache Airflow and automated with Jenkins CI/CD.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Pipeline                            │
│                                                                 │
│  Kaggle API                                                     │
│      ↓                                                          │
│  [Extract] → HDFS Raw Zone (/hdfs/{env}/youtube_data/raw_zone/) │
│      ↓                                                          │
│  [Transform] → HDFS Staging Zone (staging_zone/)                │
│      ↓                                                          │
│  [PySpark Analytics]                                            │
│      ↓              ↓                                           │
│  HDFS Processed    PostgreSQL (youtube_analytics)                │
│  Zone (parquet)        ↓                                        │
│                    Metabase Dashboard                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Orchestration: Apache Airflow (scheduled every 2 minutes)     │
│  CI/CD: Jenkins (validates code on every push)                  │
│  Infrastructure: Docker Compose (all services containerized)    │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Component        | Technology                  |
|------------------|-----------------------------|
| Orchestration    | Apache Airflow 2.9.1        |
| Processing       | PySpark 3.3.0               |
| Storage          | HDFS (Hadoop 3.2.1)         |
| Database         | PostgreSQL 13               |
| Visualization    | Metabase v0.46.6            |
| Containerization | Docker + Docker Compose     |
| CI/CD            | Jenkins                     |
| Language         | Python 3.8+                 |

## Project Structure

```
youtube-etl-project/
├── dags/
│   └── youtube_etl_dag.py          # Airflow DAG - orchestrates the pipeline (daily schedule)
├── scripts/
│   ├── config_loader.py            # Loads environment-specific config (dev/test/prod)
│   ├── makedir.py                  # Creates HDFS zone directories
│   ├── extract/
│   │   └── youtube_extract.py      # Downloads from Kaggle, uploads to HDFS raw zone
│   ├── transform/
│   │   └── clean_youtube_data.py   # Cleans data, moves to staging zone
│   └── load/
│       ├── load_to_hdfs.py         # Uploads files to HDFS
│       ├── load_to_staging.py      # Creates staging zone directory
│       └── load_to_processed.py    # Creates processed zone directory
├── spark_jobs/
│   ├── __init__.py
│   ├── process_data.py             # PySpark analytics, writes to HDFS and PostgreSQL
│   └── utils/
│       ├── __init__.py
│       └── spark_helpers.py        # Spark session and HDFS utilities
├── envs/
│   ├── dev/config.yaml             # Development environment config
│   ├── test/config.yaml            # Testing environment config
│   └── prod/config.yaml            # Production environment config
├── jenkins/
│   ├── Jenkinsfile                 # CI/CD pipeline definition
│   └── Dockerfile                  # Custom Jenkins image with Python and Docker
├── Dockerfile                      # PySpark container image
├── docker-compose.yml              # Full infrastructure stack
├── entrypoint.sh                   # Container startup (creates Kaggle credentials)
├── init-db.sql                     # Creates metabase and youtube_analytics databases
├── requirements.txt                # Python dependencies
├── LICENSE                         # MIT License
└── .gitignore                      # Protects sensitive files (.env, credentials)
```

---

## Prerequisites

- Docker Desktop (8GB+ RAM allocated)
- Git
- Kaggle account with API key

---

## Setting Up Your Credentials

This project uses a `.env` file to store sensitive credentials. This file is **never pushed to GitHub** (it is in `.gitignore`).

### Step 1: Get Your Kaggle API Key

1. Go to [kaggle.com](https://www.kaggle.com)
2. Click your profile picture → **Settings**
3. Scroll to **API** section
4. Click **"Create New Token"**
5. A file called `kaggle.json` downloads with your credentials:
   ```json
   {
       "username": "your_username",
       "key": "your_api_key"
   }
   ```

### Step 2: Create Your `.env` File

In the project root, create a file called `.env`:

```
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
ENVIRONMENT=dev
```

**Important:** Do not use quotes around the values.

### How Credentials Are Used Safely

```
.env file (on your machine only)
    ↓
docker-compose.yml reads .env
    ↓
Passes KAGGLE_USERNAME and KAGGLE_KEY to pyspark container
    ↓
entrypoint.sh creates /root/.config/kaggle/kaggle.json inside container
    ↓
Kaggle library authenticates using that file
    ↓
Downloads dataset automatically
```

Your credentials are **never stored in the Docker image** or pushed to GitHub. They exist only in your local `.env` file and temporarily inside the running container.

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Manendar/youtube-etl-project.git
cd youtube-etl-project
```

### 2. Create Your `.env` File

```
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
ENVIRONMENT=dev
```

### 3. Build and Start Services

```bash
docker-compose build --no-cache
docker-compose up -d
```

### 4. Wait for Services to Start (~60 seconds)

```bash
docker-compose ps
```

All containers should show as running:
- `hadoop_namenode`
- `hadoop_datanode`
- `pyspark-app`
- `airflow_postgres`
- `airflow_init`
- `airflow_webserver`
- `airflow_scheduler`
- `metabase`
- `jenkins_server`

### 5. Trigger the Pipeline

The pipeline is scheduled to run **automatically every day at 2:00 AM**. You can also trigger it manually:

Open Airflow at **http://localhost:8082** (admin / admin) and trigger the `youtube_etl_pipeline` DAG.

---

## Pipeline Steps

The pipeline runs 6 tasks in sequence:

```
create_hdfs_directories
    ↓
extract_raw_data
    ↓
create_staging_zone
    ↓
transform_clean_data
    ↓
create_processed_zone
    ↓
process_spark_analytics
```

| Task | What It Does |
|------|-------------|
| `create_hdfs_directories` | Creates all HDFS zone directories |
| `extract_raw_data` | Downloads dataset from Kaggle, uploads to HDFS raw zone |
| `create_staging_zone` | Creates staging directory in HDFS |
| `transform_clean_data` | Cleans CSV data (dedup, type casting, null handling), moves to staging |
| `create_processed_zone` | Creates processed directory in HDFS |
| `process_spark_analytics` | Runs PySpark analytics, saves to HDFS and PostgreSQL |

---

## Analytics Outputs

PySpark generates 5 analytics tables saved to both HDFS and PostgreSQL:

| Table | Description |
|-------|-------------|
| `top_videos_by_views` | Top 1000 trending videos by view count |
| `channel_statistics` | Aggregated stats per channel (total views, avg likes, etc.) |
| `category_performance` | Average views and likes per category |
| `trending_patterns` | Monthly trending video counts over time |
| `high_engagement_videos` | Top 1000 videos by engagement rate |

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8082 | admin / admin |
| HDFS NameNode | http://localhost:9870 | - |
| Metabase | http://localhost:3000 | set on first login |
| Jenkins | http://localhost:8080 | admin / admin |
| PySpark UI | http://localhost:4040 | - (only during Spark job) |
| PostgreSQL | terminal only | airflow / airflow |

---

## Using Each UI

### Airflow (http://localhost:8082)

Airflow orchestrates your entire pipeline.

**Trigger the pipeline manually:**
1. Go to http://localhost:8082
2. Find `youtube_etl_pipeline`
3. Click the play button (▶) to trigger
4. Watch tasks turn green one by one

**View task logs:**
1. Click on a DAG run
2. Click on any task
3. Click **"Log"** to see detailed output

**Rerun a single task:**
1. Click on a DAG run
2. Click on the task you want to rerun
3. Click **"Clear task"**

### HDFS NameNode UI (http://localhost:9870)

Browse your data lake files.

**Browse files:**
1. Go to http://localhost:9870
2. Click **Utilities** → **Browse the file system**
3. Navigate to `/hdfs/dev/youtube_data/`
4. Explore raw_zone, staging_zone, processed_zone

### Metabase (http://localhost:3000)

Build dashboards and query your analytics data.

**First time setup:**
1. Go to http://localhost:3000
2. Complete the setup wizard
3. When asked to add a database, use:
   ```
   Database type: PostgreSQL
   Host:          postgres
   Port:          5432
   Database name: youtube_analytics
   Username:      airflow
   Password:      airflow
   ```

**Run a SQL query:**
1. Click **+ New** → **SQL query**
2. Select **YouTube Analytics**
3. Write your SQL and click Run (▶)

**Example queries:**

Top 10 channels by views:
```sql
SELECT channel_title, total_views
FROM channel_statistics
ORDER BY total_views DESC
LIMIT 10
```

Category performance:
```sql
SELECT category_id, avg_views, avg_likes
FROM category_performance
ORDER BY avg_views DESC
```

Trending patterns over time:
```sql
SELECT year, month, trending_count
FROM trending_patterns
ORDER BY year, month
```

**Build a dashboard:**
1. Click **+ New** → **Dashboard**
2. Name it "YouTube Analytics Dashboard"
3. Click **+ New** → **SQL query** to create charts
4. Save each chart and add to dashboard

**Change chart type:**
1. Run your SQL query
2. Click **Visualization** button at the bottom
3. Select chart type (Bar, Line, Pie, etc.)
4. Click **Done**

### PostgreSQL (Terminal)

Query your data directly:

```bash
docker exec -it airflow_postgres psql -U airflow -d youtube_analytics
```

Useful commands:
```sql
\dt                          -- list all tables
\d channel_statistics        -- describe table structure
SELECT * FROM channel_statistics LIMIT 5;
\q                           -- exit
```

---

## Environments

This project supports three environments. Each has its own isolated data and configuration.

| Setting | Dev | Test | Prod |
|---------|-----|------|------|
| HDFS Path | /hdfs/dev/ | /hdfs/test/ | /hdfs/prod/ |
| Airflow Port | 8082 | 8083 | 8084 |
| Log Level | DEBUG | INFO | WARNING |
| Monitoring | Off | On | On |
| Alerts | Off | Off | On |

**Switch environments** by changing `ENVIRONMENT` in your `.env` file:

```
ENVIRONMENT=dev    # development (default)
ENVIRONMENT=test   # testing
ENVIRONMENT=prod   # production
```

---

## CI/CD with Jenkins

Jenkins automates the entire deployment process so you never have to manually SSH into servers and run commands.

### How Jenkins Works in This Project

**Current implementation (local development):**
```
You push code to GitHub
    ↓
Jenkins (manually triggered or via webhook):
  1. Pulls your code from GitHub
  2. Validates project structure
  3. Lints all Python files (syntax check)
  4. Builds the Docker image
  5. Runs integration tests
  6. Sends email notification (success/failure)
```

**Production implementation (dedicated Jenkins server):**
```
You push code to GitHub (feature branch)
    ↓
GitHub sends webhook to Jenkins server
    ↓
Jenkins automatically:
  1. Pulls your code
  2. Validates and lints
  3. Builds Docker images
  4. Deploys to TEST server (docker-compose up -d)
  5. Runs health checks and integration tests
  6. Notifies QA team
    ↓
QA team validates on TEST environment
    ↓
QA approves → merge to main branch
    ↓
Jenkins deploys to PROD server
```

### Setting Up Jenkins

> **Note:** In this project, Jenkins runs inside docker-compose alongside the ETL services for portability and ease of setup. In a production environment, Jenkins would run on a **dedicated server** separate from the application servers. This allows Jenkins to have full control over starting, stopping, and deploying containers on remote machines via SSH without the risk of shutting itself down.


**Step 1: Start Jenkins**
```bash
docker run -d \
  -p 8080:8080 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --name jenkins \
  jenkins/jenkins:lts
```

**Step 2: Get initial password**
```bash
docker logs jenkins | grep -A 3 "initial"
```

**Step 3: Open Jenkins at http://localhost:8080**

**Step 4: Install plugins**
- Pipeline
- Git
- Docker Pipeline

**Step 5: Create a Pipeline job**
1. Click **New Item** → **Pipeline**
2. Name it: `youtube-etl-pipeline`
3. Under Pipeline:
   - Definition: `Pipeline script from SCM`
   - SCM: `Git`
   - Repository URL: `https://github.com/your-username/youtube-etl-project`
   - Script Path: `jenkins/Jenkinsfile`
4. Click **Save**

**Step 6: Set up GitHub Webhook**
1. Go to your GitHub repo → **Settings** → **Webhooks**
2. Add webhook: `http://your-jenkins-url/github-webhook/`
3. Content type: `application/json`
4. Events: `Push events`

Now every time you push code, Jenkins automatically builds, tests, and deploys!

### Jenkins Pipeline Stages

The `jenkins/Jenkinsfile` defines these stages:

| Stage | What It Does |
|-------|-------------|
| Checkout | Pulls code from GitHub |
| Validate | Checks all required files and directories exist |
| Lint | Validates Python syntax for all .py files |
| Build Image | Builds the PySpark Docker image |
| Integration Tests | Tests HDFS connectivity and Python dependencies |
| Deploy | Reports success and service URLs |

On success or failure, Jenkins sends an email notification to the configured recipient.

### Environment Variables in Jenkins

Add these to Jenkins credentials (never hardcode them):
- `kaggle-username` - your Kaggle username (Secret text)
- `kaggle-key` - your Kaggle API key (Secret text)
- `notification-email` - email address for build notifications (Secret text)
- `github-credentials` - GitHub username + Personal Access Token (Username with password)

Jenkins passes them securely to the pipeline without exposing them in code or logs.

---

## Dataset

YouTube trending video statistics from 10 countries: US, GB, CA, DE, FR, IN, JP, KR, MX, RU.

Source: [Kaggle - Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new)

---

## Security Notes

- `.env` file is gitignored and never committed to GitHub
- `kaggle.json` is gitignored and never committed
- Kaggle credentials are created at container runtime via `entrypoint.sh` (not baked into Docker image)
- Jenkins stores credentials encrypted in its own database
- Default passwords (`admin/admin`, `airflow/airflow`) should be changed in production
- Enable HDFS permissions in production (`HDFS_CONF_dfs_permissions_enabled=true`)
- Generate a strong `AIRFLOW__WEBSERVER__SECRET_KEY` for production

---

## License

MIT License - see [LICENSE](LICENSE) file for details.
