# 🥍 Lacrosse Game Data Pipeline

## Project Overview
This project is a technical assessment designed to demonstrate skills in building a data pipeline within a distributed environment. The core task is to process a simulated lacrosse game stream, generate a real-time box score, and update a database with final game statistics.

The pipeline is built using **PySpark** and **Apache Drill** to handle data from a variety of sources, including a simulated game stream from an **S3-compatible object store**, and player and team reference data from a **Microsoft SQL Server database**.

## ⚙️ Technical Environment
The entire environment is containerized using `docker-compose`, which sets up the following services:

* **`mssql`**: A Microsoft SQL Server database (`sidearmdb`) storing `players` and `teams` reference data.
* **`minio`**: An S3-compatible object store.
    * `gamestreams` bucket: Contains the live game stream file (`gamestream.txt`).
* **`mongodb`**: Stores the real-time game box score in the `sidearm.boxscores` collection.
* **`drill`**: An instance of Apache Drill for querying and exploring data across different sources.
* **`jupyter`**: A Jupyter Lab instance for developing and running PySpark code.
* **`gamestream`**: A Python script that simulates the live lacrosse game, appending events to `gamestream.txt` in the `minio` S3 bucket.

## 📈 Data Pipeline & Transformations
The project involves a two-part data pipeline:

### 1. Real-Time Box Score Generation
* **Objective**: At any point during the game, process the `gamestream.txt` to create a real-time box score.
* **Process**: The pipeline reads the game events, groups them by player and team, and calculates in-game statistics (shots, goals, shooting percentage). This data is then combined with player and team reference information.
* **Output**: The final box score is transformed into a **JSON document** and written to the `mongodb/sidearm/boxscores` collection, keyed by the latest game event ID. This allows web developers to display live game statistics.

### 2. Post-Game Database Update
* **Objective**: After the game concludes (event ID `-1`), update the player and team statistics to reflect the final outcome.
* **Process**: The final game statistics are aggregated. The `players` and `teams` data frames are loaded from SQL Server, joined with the final stats, and updated.
* **Output**: The updated player and team data is written to new tables, `players2` and `teams2`, in the `mssql` database. This approach avoids row-level updates, which are inefficient in distributed big data environments.

## 🎯 Key Achievements
* Designed and implemented a complete data pipeline using PySpark.
* Performed **data ingestion** from a live stream file and an external SQL database.
* Executed **advanced data transformations** including grouping, aggregation, and joining across different data sources.
* Demonstrated an understanding of **distributed data processing** best practices, such as avoiding row-level updates.
* Generated a **real-time JSON output** for a web-based application and updated a database for long-term data persistence.

## 📦 Getting Started
To run this project, clone the repository and use `docker-compose` to start the environment:

```bash
# Clone the repository
git clone [https://github.com/mafudge/ist769sp24midterm.git](https://github.com/mafudge/ist769sp24midterm.git)
cd ist769sp24midterm

# Start the environment
docker-compose up -d
