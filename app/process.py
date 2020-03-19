from spark_instance import SparkFactory


def run_job(input):
    """
    Run Spark Job. Data wrangling process
    """
    spark = SparkFactory.get_instance('local')

    # Load datasets
    df = spark.read_data(input)

    df.show()

    spark.stop()
