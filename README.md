# 🚀 Dynamic Raw Data ETL to AWS S3 with Apache Spark
> A hands-on cloud data engineering project showcasing automated ETL, AWS S3 data lakes, and Apache Spark on EC2.


## 📖 Overview
This project demonstrates an **ETL pipeline** for ingesting raw stock data into an **AWS S3 data lake** using **Apache Spark** on an EC2 instance.  
It is designed as a foundation for scalable, cloud-based data engineering workflows.

The system:
- Reads raw data files from a local EC2 directory
- Transforms and cleans the data using **PySpark**
- Writes structured data to an **S3 bucket**
- Includes a lightweight **bash script watcher** to automate ingestion when new files appear

---

## 🛠️ Tech Stack
- **AWS**: EC2 (compute), S3 (storage)
- **Apache Spark (PySpark)**: ETL and data processing
- **Linux Bash**: File monitoring and automation
- **Git/GitHub**: Version control and collaboration

---

## ⚙️ Features
- Automated raw data ingestion from EC2 to S3
- Spark-based transformations for scalability
- File watcher for near real-time updates
- Configurable bucket/directory paths
- Logs tracked locally
- Future-ready for Apache Kafka for real-time streaming ingestion.
- Future-ready for Delta Lake for ACID transactions, schema evolution, and versioning

---

## 🚀 How It Works
1. `rawdata_to_s3.py`  
   - Initializes a Spark session  
   - Loads raw input data  
   - Applies ETL transformations  
   - Writes results to S3  

2. `rawdata_watcher.sh`  
   - Monitors the local directory (`/home/ec2-user/raw_data`)  
   - Triggers ETL job when new data arrives  

3. **S3 Output**  
   - Data is stored in `s3://mvp-data-lake-jrodriguez/etl_stock_data/`

---

### 📂 Project Structure
```bash
├── rawdata_to_s3.py       # Spark ETL script
├── rawdata_watcher.sh     # Bash script for monitoring new files
├── logs/                  # Local log files (gitignored)
└── README.md              # Project documentation
```

---

### 💻 Usage
1. Run ETL directly
```bash
/opt/spark/bin/spark-submit \
  --master local[2] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  rawdata_to_s3.py
```
2. Start the watcher service
```bash
# Start service
sudo systemctl start rawdata-watcher.service

# Check logs
tail -f /home/ec2-user/scripts/logs/rawdata_watcher_service.log
```
3. Update EC2 Raw-Data (triggers watcher ETL job)
```bash
scp -v -i <path-to-local-keypair.pem> <path-to-updated-QQQ-csv-file> <ec2-user-account>@<public-IPv4-DNS>.compute-1.amazonaws.com:/home/ec2-user/raw_data/archived_QQQ_results.csv
```
4. Stop the watcher
```bash
sudo systemctl stop rawdata-watcher.service
```

---

## 📸 Sample Output

### S3 Data Lake Structure
![S3 bucket structure](scripts/images/Bucket_Structure.png)

### ETL Logs (successful ingestion)
![ETL log output](scripts/images/ETL_Logs.png)