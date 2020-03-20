from spark_instance import SparkFactory


def run_job(input_data):
    """
    Run Spark Job. Data wrangling process
    """
    spark = SparkFactory.get_instance('local')

    # Load datasets
    df = spark.read_data(input_data)
    print("Input path: {}".format(input_data))

    df.show()

    spark.stop()
