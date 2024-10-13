# Airflow SFTP Sync Project

This project provides **Apache Airflow DAGs** to **synchronize files between SFTP servers** or between other storage solutions (e.g., **S3**, **local storage**). It aims to offer flexibility by abstracting connection types so you can easily switch between SFTP, S3, or other services without refactoring your code.

---

## Table of Contents
1. [Project Overview](#project-overview)  
2. [Prerequisites](#prerequisites)  
3. [Installation and Setup](#installation-and-setup)  
4. [DAG Files](#dag-files)  
5. [Assumptions and Trade-offs](#assumptions-and-trade-offs)  
6. [Additional Plugins](#additional-plugins)  

---

## Project Overview
This project provides:
- **Airflow DAGs** for **file synchronization** between 2 SFTP storages.
- A **StorageOperator abstraction** to handle connections uniformly, making it easy to switch between storages for later.
- **TaskFlow API integration** to ensure cleaner code with reusable components.

---

## Prerequisites
- **Docker**: Ensure Docker is installed for containerized Airflow setup.  
  [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose**: Required to orchestrate multiple containers (Airflow webserver, scheduler, database).  
  [Install Docker Compose](https://docs.docker.com/compose/install/)  
- **Airflow 2.10.2**: This is stable version.
- **Python 3.12**: Latest airflow-python version on Docker hub.

---

## Installation and Setup

### 1. Clone the Repository
```bash
git https://github.com/ThadaPhan/sftp-pipeline.git
cd sftp-pipeline
```
### 2. Create a `.env` File

Create a `.env` file in the root directory of the project with the following content:

```bash
# Ref: https://hub.docker.com/r/atmoz/sftp/
SFTP_USER_1="user:pass[:e][:uid[:gid[:dir1[,dir2]...]]]"
SFTP_USER_1="user:pass[:e][:uid[:gid[:dir1[,dir2]...]]]"

# Ref: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
AIRFLOW_UID=1000
_AIRFLOW_WWW_USER_USERNAME=...
_AIRFLOW_WWW_USER_PASSWORD=...
AIRFLOW_CONN_SFTP_SERVER_1=... # Depends on sftp1 config above 
AIRFLOW_CONN_SFTP_SERVER_2=... # Depends on sftp2 config above 
```
### 3. Create docker network (bridge driver)
```bash
docker network create airflow-network
```
This only an example, but if you change it, you have to change the default network on docker-composer-*.yaml


### 4. Build and Start the Airflow Docker Compose
```bash
docker-compose -f docker-compose-sftp.yaml up -d
docker-compose -f docker-compose-airflow.yaml up -d
```

### 5. Access the Airflow UI
+ Url: http://localhost:8080
+ Login using the default credentials:
    + Username: <_AIRFLOW_WWW_USER_USERNAME>
    + Password: <_AIRFLOW_WWW_USER_PASSWORD>

### 6. Create airflow connnection to 2 sftp server
+ Source: `sftp1`
+ Destination: `sftp2`
+ Because this only a simple pipeline so we can hard code to create it, but for larger project, we should create with programmatically

### 7. Troubleshooting

```bash
docker logs <service_name>
```

---


## DAG Files


```python

    # Get metadata of 2 SFTP server
    source_describe = describe_path_on_source()
    dest_describe = describe_path_on_destination()

    # Detect change directory and file 
    change_path = get_change_path(
        source_describe,
        dest_describe
    )

    # Sync all change directory before
    sync_directory(change_path)

    # Sync all change file after
    sync_file(change_path)

```

---
## Assumptions and Trade-offs

+ How to detect the changes: edited_time or size
+ Answer: Both
+ In the first time, every people will think only the edited_time is enough, but:
    + When server change the default time zone, every thing will be breaked and nothing can make sure our pipeline still ok.
+ So, we need another attribute: size.


---

## Additional Plugins
+ [Polars](https://pola.rs/)
