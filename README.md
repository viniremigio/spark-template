# Data Engineer Challenge

## Requirements
- Spark 2.4.5
- Python libs: pweave, Bokeh and boto3.

If you run this on Amazon EMR, you should install pweave and Bokeh at cluster bootstrap time.

## Usage
```
spark-submit src/process.py <nyc_json_file> <vendor_csv_file> <payment_csv_file> <output_bucket> <output_path_to_JSON_and_HTML>
```

If you run this script on EMR, just add the spark submit as a EMR Step. The script also need to be uploaded on S3.

## How the script works?
- Read data (from S3 or locally)
- Process distributed with Spark, generating required metrics
- Save JSON file with the analysis in S3 bucket
- Generate a HTML report using pweave and Bokeh
- Upload the generated report to S3

## Report analysis
- [Download Report](report/report.html)
- [Download Spark Job output - JSON file](report/output.json)

## TODO
- Change Bokeh script to generate better graphs
- AWS Cloudformation to bootstrap EMR Cluster
- Logs
- Unit tests