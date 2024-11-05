from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName("GCP GCS Read & Write to BigQuery").getOrCreate()
    sc=spark.sparkContext
    sc.setLogLevel("ERROR")
    conf=sc._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    
    print("Usecase 5: Spark application to read data from GCS and load to BQ(Raw) and load into another BQ(curated) table in the GCP")
    print("1.Ingestion layer data read from GCS - brought by some data producers")
    gcs_df=spark.read.option("header","false").option("inferSchema","true")\
        .csv("gs://prasana-wrkoutdata/data/custs").toDF("custno","firstname","lastname","age","profession")
    gcs_df.show(10)
    print("GCS read completed successfully")
    print("Ensure to create the BQ datasets (rawsds & curatedds)->table creation(option")
    
    print("2.Write GCS data to Raw BQ table")
    gcs_df.write.mode("overwrite").format('com.google.cloud.spark.bigquery.BigQueryRelationProvider')\
    .option("temporaryGCSBucket",'prasana-wrkoutdata/tmp')\
    .option('table',rawds.customer_raw').save()
    
    print("3.Writing GCS data to curated BQ table")
    gcs_df.createOrReplaceTempView("raw_view")
    curated_bq_df=spark.sql("select custno,concat(firstname,',',lastname)as name,age,coalesce(profession,'unknown') as profession from rawds.customer_raw where age>30")
    print("Read from rawds is completed")
    curated_bq_df.write.mode("overwrite").format("com.google.cloud.spark.bigquery.BigQueryRelationProvider")\
    .option("temporaryGCSBucket",'prasana-wrkoutdata/tmp')\
    .option('table','curatedds.customer_curated').save()
    print("GCS to Curated BQ table load completed")
    
main()
   
    
    
    
    
    
    
