import sys
import json
import boto3
import subprocess
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import avg, udf
from pyspark.sql.types import StringType, IntegerType, DoubleType


def run_job(nyc_trips_path, vendor_info_path, payment_info_path, bucket, output_key_path):
    """
    Run Spark Job. Data wrangling process
    """
    response_dict = {}

    # Spark conf setup to run locally
    spark_conf = SparkConf()
    spark_conf.setAppName('Taxi trips job')
    spark_conf.setMaster('local[6]')
    spark_conf.set('spark.driver.memory', '6g')

    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    spark = SparkSession(sparkContext=sc).builder.getOrCreate()

    # Load datasets
    df_taxi_trips = spark.read.json(nyc_trips_path)
    df_vendor_lookup = spark.read.option("header", "true").csv(vendor_info_path)
    df_payment_lookup = spark.read.option("header", "true").csv(payment_info_path)

    # 1. What is the average distance traveled by trips with a maximum of 2 passengers;
    avg_distance = df_taxi_trips.filter("passenger_count<=2").agg(avg("trip_distance"))
    avg_distance_km = round(avg_distance.collect()[0]["avg(trip_distance)"], 2)
    response_dict.update({"avg_distance_km": avg_distance_km})

    # 2. Which are the 3 biggest vendors based on the total amount of money raised;
    df_cash_payment = df_payment_lookup.filter("B=='Cash'")\
                     .withColumnRenamed("A", "payment_type")\
                     .withColumnRenamed("B", "payment_lookup")

    df_taxi_trips = df_taxi_trips.join(df_cash_payment, how="inner", on=["payment_type"])
    df_taxi_trips = df_taxi_trips.join(df_vendor_lookup.select("vendor_id", "name"), how="inner", on="vendor_id")

    df_taxi_trips.createOrReplaceTempView("taxi_trips")
    df_top_vendors = spark.sql("""SELECT name, round(sum(total_amount), 2) as money_raised 
                                  FROM taxi_trips 
                                  WHERE payment_lookup = 'Cash' 
                                  GROUP BY name ORDER BY money_raised DESC LIMIT 3""")

    response_dict.update({"top_vendors": list(map(lambda row: row.asDict(), df_top_vendors.collect()))})

    # 3. Make a histogram of the monthly distribution over 4 years of rides paid with cash;
    udf_get_year_month = udf(lambda pickup_date: get_year_month_udf(pickup_date), StringType())
    df_taxi_trips = df_taxi_trips.withColumn("year_month", udf_get_year_month(df_taxi_trips.pickup_datetime))
    df_taxi_trips.createOrReplaceTempView("taxi_trips")
    df_hist_rides = spark.sql("""SELECT year_month, count(pickup_datetime) as num_rides
                                 FROM taxi_trips 
                                 WHERE payment_lookup = 'Cash' 
                                 GROUP BY year_month ORDER BY year_month ASC""")

    response_dict.update({"hist_rides": list(map(lambda row: row.asDict(), df_hist_rides.collect()))})

    # 4. Make a time series chart computing the number of tips each day for the last 3 months of 2012.
    udf_get_year_month_day = udf(lambda pickup_date: get_day_udf(pickup_date), StringType())
    udf_number_of_tips = udf(lambda tip: 1 if tip > 0.0 else 0, IntegerType())

    df_taxi_trips = df_taxi_trips.withColumn("year_month_day", udf_get_year_month_day(df_taxi_trips.pickup_datetime))\
                                 .withColumn('hasTips', udf_number_of_tips(df_taxi_trips.tip_amount))
    df_taxi_trips.createOrReplaceTempView("taxi_trips")
    df_ts_tips = spark.sql("""SELECT year_month_day, count(hasTips) as num_tips
                              FROM taxi_trips 
                              WHERE year_month_day >= to_date('2012-10-01', 'yyyy-MM-dd') 
                              AND year_month_day <= to_date('2012-12-31', 'yyyy-MM-dd')
                              GROUP BY year_month_day ORDER BY year_month_day ASC""")

    response_dict.update({"ts_tips": list(map(lambda row: row.asDict(), df_ts_tips.collect()))})

    # 5. What is the average trip time on Saturdays and Sundays;
    udf_get_weekday = udf(lambda pickup_date: get_weekday(pickup_date), StringType())
    udf_trip_time = udf(lambda dropoff, pickup: get_trip_time(dropoff, pickup), DoubleType())

    df_taxi_trips = df_taxi_trips.withColumn("weekday", udf_get_weekday(df_taxi_trips.pickup_datetime))\
                                 .withColumn("trip_time",
                                             udf_trip_time(df_taxi_trips.dropoff_datetime,
                                                           df_taxi_trips.pickup_datetime))

    df_taxi_trips.createOrReplaceTempView("taxi_trips")
    df_weekend_avg_trips = spark.sql("""SELECT weekday, round(avg(trip_time),2) as avg_trip_time_min
                                        FROM taxi_trips
                                        WHERE weekday = 'Saturday' OR weekday = 'Sunday'
                                        GROUP BY weekday ORDER BY weekday ASC""")

    response_dict.update({"weekend_avg_trips": list(map(lambda row: row.asDict(), df_weekend_avg_trips.collect()))})

    # Save JSON to S3
    client = boto3.client('s3')
    response = client.put_object(Bucket=bucket,
                                 Body=json.dumps(response_dict),
                                 Key='{}/report_data.json'.format(output_key_path))

    print("JSON Object uploaded. ETag=" + response['ETag'])

    spark.stop()


# Auxiliary UDFs ###########################################################


def extract_date(pickup_datetime):
    try:
        return datetime.strptime(pickup_datetime, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        return datetime.strptime(pickup_datetime, "%Y-%m-%dT%H:%M:%S%z")


def get_year_month_udf(pickup_datetime):
    date = extract_date(pickup_datetime)
    return str(date.year) + '-' + str(date.month).zfill(2)


def get_day_udf(pickup_datetime):
    year_month = get_year_month_udf(pickup_datetime)
    date = extract_date(pickup_datetime)
    year_month_day = year_month + '-' + str(date.day).zfill(2)
    return year_month_day


def get_weekday(pickup_datetime):
    date = extract_date(pickup_datetime)
    return date.strftime("%A")


def get_trip_time(dropoff, pickup):
    pickup_date = extract_date(pickup)
    dropoff_date = extract_date(dropoff)
    minutes = round((dropoff_date.timestamp() - pickup_date.timestamp()) / 60, 2)
    return minutes


if __name__ == '__main__':

    # Get info about the files
    nyc_trips = sys.argv[1]
    vendor_info = sys.argv[2]
    payment_info = sys.argv[3]
    bucket_path = sys.argv[4]
    output_path = sys.argv[5]

    run_job(nyc_trips, vendor_info, payment_info, bucket_path, output_path)

    # Generate report
    cmd = ['pweave', '-f', 'md2html', 'report/report.pmd']
    proc = subprocess.Popen(cmd)
    proc.communicate()

    # Upload HTML report to S3
    cmd = ['aws', 's3', 'cp', 'report/report.html', 's3://{}/{}/'.format(bucket_path, output_path)]
    proc = subprocess.Popen(cmd)
    proc.communicate()

    print("Done!")
