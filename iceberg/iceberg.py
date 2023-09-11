'''
%%tags
{
    "lm_troux_uid":"7B962901-4D59-400C-B089-403B614DCDC3",
    "deployment_guid":"27c934a3-ed8a-4ecf-ada4-c3699d8143e9"
}
'''

import sys
import timeit
import logging
import pyspark
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import multiprocessing
import pandas
import concurrent.futures as cf
from awsglue.dynamicframe import DynamicFrame
glueContext = GlueContext(SparkContext.getOrCreate())
sparksession = glueContext.spark_session

conf = (
    pyspark.SparkConf()
        .setAppName('poc_iceberg')
  		#packages
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.0.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
  		#SQL Extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
  		#Configuring Catalog
        .set('spark.sql.catalog.glue', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.glue.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
        .set('spark.sql.catalog.glue.warehouse', 's3://lc-aug1-2023/Iceberg/warehouse/')
        .set('spark.sql.catalog.glue.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set("spark.sql.defaultCatalog", 'glue')
        .set("spark.sql.catalogImplementation", 'in-memory')
)
spark = sparksession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

inputDynamicframe = glueContext.create_dynamic_frame_from_options(connection_type ="s3",connection_options = {"paths": ["s3://lc-aug1-2023/hudi/source/"], "recurse": True },format = "csv", format_options={"withHeader": True}, transformation_ctx ="dyf")
inputDf = inputDynamicframe.toDF()
inputDf.show()
inputDf.createOrReplaceTempView("pilot_view")

spark.sql("CREATE TABLE IF NOT EXISTS glue.default.pilot_table AS (SELECT * FROM pilot_view)")
dataFrame = spark.read.format("iceberg").load("glue.default.pilot_table")
target_path = "s3://lc-aug1-2023/Iceberg/table/"
dataFrame.write.mode('append').csv(target_path,header = 'true')

job.commit()