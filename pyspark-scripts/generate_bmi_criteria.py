from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql import functions as F

schema = StructType([
    StructField('iso_code', StringType(), True),
    StructField('continent', StringType(), True),
    StructField('location', StringType(), True),
    StructField('date', StringType(), True),
    StructField('total_cases', IntegerType(), True),
    StructField('total_deaths', IntegerType(), True)
])


if __name__ == '__main__':
    spark = SparkSession\
            .builder\
            .appName("GenerateBMICriteria")\
            .getOrCreate()

    df = spark.read.schema(schema).options(header=True).csv('/opt/bitnami/spark/pyspark-datasets/owid-covid-data.csv')




    df.write.mode("overwrite").parquet('/opt/bitnami/spark/pyspark-datasets/registro_covid')
   
