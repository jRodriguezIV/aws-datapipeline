# aws-datapipeline
 ---TO BE UPDATED---

<!-- rawdata_to_s3.py -->
``` bash
spark-submit \
    --master local[2] \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /home/ec2-user/scripts/rawdata_to_s3.py
```