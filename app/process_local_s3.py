from spark_instance import SparkFactory


def run_job(input_data, access_key, secret_key):
    """
    Run Spark Job. Data wrangling process
    """
    spark = SparkFactory.get_instance('local_s3', access_key, secret_key)

    # Load datasets
    df = spark.read_data(input_data)
    print("Input path: {}".format(input_data))

    df.show()

    spark.stop()
