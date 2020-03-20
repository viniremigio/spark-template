from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class SparkFactory:

    @staticmethod
    def get_instance(type, access_key=None, secret_key=None):
        if type == 'local':
            spark = SparkLocal()
        elif type == 'local_s3':
            spark = SparkLocalS3Storage(access_key, secret_key)
        return spark


class Spark:
    """
    Superclass Spark
    """
    def read_data(self, input_data):
        df = self.session.read.option('header', 'true').csv(input_data)
        return df

    def write_data(self):
        pass

    def stop(self):
        self.session.stop()


class SparkLocal(Spark):
    """
    SparkLocal: Instantiate Spark locally. Read and Write data into local storage
    """
    def __init__(self):
        spark_conf = SparkConf()
        spark_conf.setAppName('Taxi trips job')
        spark_conf.setMaster('local[6]')
        spark_conf.set('spark.driver.memory', '6g')

        sc = SparkContext(conf=spark_conf)
        sc.setLogLevel("ERROR")
        self.session = SparkSession(sparkContext=sc).builder.getOrCreate()


class SparkLocalS3Storage(SparkLocal):
    """
    SparkLocalS3Storage: Instantiate Spark locally. Read and Write data into S3. AWS Credentials required
    """
    def __init__(self, access_key, secret_key):
        super().__init__()
        hadoop_config = self.session._jsc.hadoopConfiguration()

        hadoop_config.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hadoop_config.set("fs.s3n.awsAccessKeyId", access_key)
        hadoop_config.set("fs.s3n.awsSecretAccessKey", secret_key)

