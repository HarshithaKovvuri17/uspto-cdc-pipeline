# 🚀 USPTO Patent CDC Pipeline

### Real-Time Change Data Capture (CDC) Pipeline using Debezium, Apache Kafka, PostgreSQL, and Python

## 📌 Overview

The USPTO Patent CDC Pipeline is a real-time data engineering project that demonstrates Change Data Capture (CDC) using PostgreSQL, Debezium, Apache Kafka, Kafka Connect, and Python.

The pipeline captures INSERT, UPDATE, and DELETE operations from a source PostgreSQL database, streams those changes through Kafka, and synchronizes them into a target PostgreSQL database while maintaining both:

- Current State Table (Latest Records)
- Historical Table (SCD Type 2)

This project showcases modern event-driven architecture and real-time data replication commonly used in enterprise-grade data engineering systems.

---

## 🎯 Project Objectives

✅ Capture database changes in real time

✅ Stream changes using Apache Kafka

✅ Synchronize source and target databases

✅ Maintain complete historical tracking using SCD Type 2

✅ Demonstrate event-driven architecture

✅ Build an end-to-end CDC data pipeline

---

## 🏗️ Architecture

```text
PostgreSQL Source Database
            │
            ▼
     WAL (Logical Decoding)
            │
            ▼
         Debezium
            │
            ▼
       Apache Kafka
            │
            ▼
      Python Consumer
            │
            ▼
PostgreSQL Target Database
 ├── patent_current_state
 └── patent_history
```

---

## ✨ Key Features

- Real-Time Change Data Capture (CDC)
- PostgreSQL Logical Replication
- Debezium PostgreSQL Connector
- Apache Kafka Event Streaming
- Kafka Connect Integration
- Python-Based CDC Consumer
- Slowly Changing Dimension (SCD Type 2)
- Historical Data Tracking
- Dockerized Infrastructure
- Automated Connector Registration
- Fault-Tolerant Event Processing

---

## 🛠️ Tech Stack

| Component | Technology |
|------------|------------|
| Database | PostgreSQL 15 |
| CDC Engine | Debezium |
| Message Broker | Apache Kafka |
| Coordination Service | ZooKeeper |
| Connector Framework | Kafka Connect |
| Consumer Service | Python |
| Containerization | Docker |
| Orchestration | Docker Compose |

---

## 📂 Project Structure

```text
uspto-cdc-pipeline/
│
├── docker-compose.yml
├── .env
├── .env.example
│
├── config/
│   └── register-postgres-connector.json
│
├── consumer/
│   ├── consumer.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── init-db/
│   ├── init-source.sql
│   ├── init-target.sql
│   └── patents.csv
│
├── verify.sh
├── verify.ps1
├── testing.md
└── README.md
```

---

## 🗄️ Database Design

### Source Table

```sql
public.patent
```

| Column | Type |
|----------|--------|
| id | VARCHAR |
| title | TEXT |
| num_claims | INTEGER |

### Target Tables

#### patent_current_state

Stores the latest version of each patent record.

#### patent_history

Stores historical versions of records for audit tracking.

| Column | Description |
|----------|-------------|
| history_id | Unique History Record |
| id | Patent ID |
| title | Patent Title |
| num_claims | Number of Claims |
| valid_from | Start Timestamp |
| valid_to | End Timestamp |

---

## 🔄 CDC Workflow

### Step 1

A user performs INSERT, UPDATE, or DELETE operations on the source PostgreSQL database.

### Step 2

PostgreSQL writes changes to the Write Ahead Log (WAL).

### Step 3

Debezium continuously monitors WAL changes.

### Step 4

Debezium publishes CDC events to Kafka topics.

### Step 5

Python Consumer subscribes to Kafka CDC topics.

### Step 6

Consumer processes events and updates:

- patent_current_state
- patent_history

### Step 7

Target database remains synchronized in real time.

---

## 📊 SCD Type 2 Implementation

### INSERT

A new record is inserted into:

- patent_current_state
- patent_history

```text
valid_from = CURRENT_TIMESTAMP
valid_to   = NULL
```

### UPDATE

Current history record is closed:

```text
valid_to = CURRENT_TIMESTAMP
```

A new active record is inserted:

```text
valid_from = CURRENT_TIMESTAMP
valid_to   = NULL
```

### DELETE

Record is removed from current state table and history is closed:

```text
valid_to = CURRENT_TIMESTAMP
```

Historical data remains preserved.

---

## 🚀 Getting Started

### Prerequisites

- Docker Desktop
- Docker Compose

Verify installation:

```bash
docker --version
docker compose version
```

### Clone Repository

```bash
git clone <repository-url>
cd uspto-cdc-pipeline
```

### Configure Environment

```bash
cp .env.example .env
```

### Start Services

```bash
docker compose up --build -d
```

### Verify Containers

```bash
docker ps
```

Expected Containers:

```text
postgres-source
postgres-target
zookeeper
kafka
kafka-connect
consumer-service
connector-creator
```

---

## 🧪 Testing CDC Operations

### INSERT Test

```sql
INSERT INTO public.patent
VALUES (
'10000005',
'Method and apparatus for quantum error correction',
10
);


## 🎯 Real-World Applications

- Data Warehousing
- Data Replication
- Event-Driven Systems
- Audit Logging
- Financial Data Pipelines
- Government Data Processing
- Healthcare Data Synchronization
- Regulatory Compliance Systems

---

## 💡 Skills Demonstrated

### Data Engineering

- Change Data Capture (CDC)
- Data Streaming
- ETL / ELT Pipelines
- Event-Driven Architecture

### Backend Development

- Python
- PostgreSQL
- Kafka Consumers

### Infrastructure

- Docker
- Docker Compose
- Distributed Systems

### Data Warehousing

- Slowly Changing Dimensions (SCD Type 2)
- Historical Data Tracking
- Audit Trail Management

---

## 📚 Learning Outcomes

This project provided hands-on experience with:

- PostgreSQL Logical Replication
- Debezium CDC Connectors
- Apache Kafka Ecosystem
- Kafka Connect
- Python Event Consumers
- Dockerized Deployments
- Real-Time Data Synchronization
- Historical Data Warehousing
- Event-Driven Data Engineering

---

## 👨‍💻 Author

**Kovvuri Harshitha**
- Email: harshitahanisha@gmail.com 
- Github Url: https://github.com/HarshithaKovvuri17/uspto-cdc-pipeline.git
---

## ⭐ Support

If you found this project useful, consider giving it a ⭐ on GitHub and sharing your feedback.