from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class SparkFactory:

    @staticmethod
    def get_instance(type):
        if type == 'local':
            spark = SparkLocal()
        return spark


class Spark:
    """
    Superclass Spark
    """
    def get_instance(self):
        pass

    def read_data(self, input_data):
        pass

    def write_data(self):
        pass

    def stop(self):
        pass


class SparkLocal(Spark):

    def __init__(self):
        spark_conf = SparkConf()
        spark_conf.setAppName('Taxi trips job')
        spark_conf.setMaster('local[6]')
        spark_conf.set('spark.driver.memory', '6g')

        sc = SparkContext(conf=spark_conf)
        sc.setLogLevel("ERROR")
        self.session = SparkSession(sparkContext=sc).builder.getOrCreate()

    def read_data(self, input_data):
        df = self.session.read.option('header', 'true').csv(input_data)
        return df

    def stop(self):
        self.session.stop()
