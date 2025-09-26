#!/bin/bash


# Load environment variables form .env file
if [ -f /home/ec2-user/.env ]; then
    set -a
    source /home/ec2-user/.env
    set +a
fi

# Infinite loop watching for new/modified CSVs
inotifywait -m -e close_write,create,delete,move "$LOCAL_DATA_DIR" --format '%w%f' | \
while read FILE
do
    if [[ "$FILE" == *.csv ]]; then
        echo "$(date): Change detected in $FILE, running ETL..." >> "$WATCH_LOG_FILE"
        
        /opt/spark/bin/spark-submit \
            --master local[2] \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            "$ETL_SCRIPT" >> "$WATCH_LOG_FILE" 2>&1

        echo "$(date): ETL finished for $FILE" >> "$WATCH_LOG_FILE"
    fi
done