from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName("GCP GCS Hive Read/Write ")\
        .enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sc=spark.sparkContext
    conf=sc._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    print("Use spark application to read csv data from GCS and get a DF created with the GCS data in the on prem,"
          "convert csv to json in the on prem DF and store the json into new GCS location")
    print("Hive to GCS to hive start here")
    custstruct1=StructType([StructField("id",IntegerType(),False),
                           StructField("fname",StringType(),False),
                           StructField("lname",StringType(),True),
                            StructField("age",ShortType(),True),
                            StructField("prof",StringType(),True)])
    gcs_df=spark.read.csv("gs://prasana-wrkoutdata/data/custs",mode="dropmalformed",schema=custstruct1)
    gcs_df.show(10)
    print("GCS Read completed successfully")

    gcs_df.write.mode("overwrite").partitionBy("age").saveAsTable("default.cust_info_gcs")
    print("GCS to (dataproc) hive table load completed successfully")

    print("Hive to GCS usecase starts here")
    gcs_df=spark.read.table("default.cust_info_gcs")
    curts=spark.createDataFrame([1],IntegerType()).withColumn("curts",current_timestamp())\
        .select(date_format(col("curts"),"yyyyMMddHHmmSS")).first()[0]
    print(curts)
    gcs_df.repartition(2).write.json("gs://inceptez-data-store-prasana/output/cust_output_json_"+curts)
    print("gcs write completed succesfully")

    print("Hive to GCS usecase starts here")
    gcs_df=spark.read.table("default.cust_info_gcs")
    curts=spark.createDataFrame([1],IntegerType()).withColumn("curts",current_timestamp())\
        .select(date_format(col("curts"),"yyyyMMddHHmmSS")).first()[0]
    print(curts)
    gcs_df.repartition(2).write.mode("overwrite").csv("gs://inceptez-data-store-prasana/output/cust_csv")
    print("GCS write completed successfully")

main()

#spark-submit --jars /home/hduser/install/gcp/gcs-connector-latest-hadoop2.jar /home/hduser/GCPlearn/usecase_4.py