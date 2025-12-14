# Kafka → ADLS Gen2 Exporter

This project is a Spring Boot application that consumes Kafka messages in batches and writes them to **Azure Data Lake Storage Gen2** using **SAS authentication**.
Each Kafka poll is aggregated and stored in a **new uniquely generated ADLS file**, ensuring no overwrite and clean data partitioning.

---

## 🚀 Features

* Batch Kafka consumption (`@KafkaListener` with `batch = true`)
* Concatenation of all messages from a single poll
* ADLS Gen2 write using SAS token
* One file created per poll (never overwritten)
* Partitioning by date:

  ```
  /kafka-export/date=YYYY-MM-DD/messages-<uuid>.log
  ```

---

## 🧩 Technologies

* Spring Boot 3
* Spring Kafka
* Azure Storage File DataLake SDK
* Azure SAS Token Authentication

---

## ⚙️ Configuration

Set your values in `application.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: your-kafka:9092
    listener:
      type: batch

app:
  kafka:
    topic: my-topic

  adls:
    account-name: <account>
    filesystem: <container>
    base-path: kafka-export
    sas-token: "<sas-token-without-question-mark>"
```

---

## ▶️ Running the Project

```bash
mvn spring-boot:run
```

The application will:

1. Poll messages from Kafka
2. Aggregate them
3. Create a unique file under the partitioned date directory
4. Upload the content to ADLS Gen2 via DFS endpoint

---

## 📂 Output Example

```
kafka-export/
└── date=2025-12-14/
    ├── messages-20251214-8ab3e0c6.log
    ├── messages-20251214-c1ff294d.log
    └── messages-20251214-f92a7e13.log
```

Each file corresponds to one Kafka poll.

---

## 📜 License

MIT
