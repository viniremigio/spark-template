# Spark Template

## Introduction

The main idea is provide bootstrap code to run Spark over the following platforms

- Run local and **read/write** files **locally**
- Run local and **read/write** files using **Amazon S3**
- EMR
- AWS Glue

## How to run

### Spark Local: Read and Write locally
```
spark-submit app/run.py file://<qualified file path>
```

### Spark Local: Read and Write using Amazon S3
```
spark-submit --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 app/run_local_s3.py s3n://<bucket_name>/<folder>/<file_name> <ACCESS_KEY> <SECRET_KEY>
```

## Bootstrap EMR Cluster

- [AWS Cloudformation](automation/cloudformation/README.md)
- [Terraform](TODO)

## TODO
- Refactor: Run locally; Local and S3 save options **[OK]**
- Run Spark using AWS Glue
- Parameterize SparkConf using resource file
- Bootstrap EMR using Cloudformation **[OK]**
- Bootstrap EMR using Terraform
- Spark Unit tests
