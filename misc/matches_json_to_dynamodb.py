import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

"""
Please pass s3 location of spark-dynamodb_2.12-1.1.2.jar in dependecy jar path while running this glue job
Please pass below job parameters:
--DYNAMODB_TABLE (table to load)
--JOB_NAME
--S3_SOURCE_BUCKET (bucket name where matches.json is)
--S3_SOURCE_PREFIX (path inside bucket of matches.json)
"""

## @params: ['JOB_NAME', 'S3_SOURCE_BUCKET', 'S3_SOURCE_PREFIX', 'DYNAMODB_TABLE']
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_SOURCE_BUCKET', 'S3_SOURCE_PREFIX', 'DYNAMODB_TABLE'])
s3_source_bucket =  args['S3_SOURCE_BUCKET'] # "iplcricketinfo"
s3_source_prefix = args['S3_SOURCE_PREFIX'] # "input_files/matches.json"
dynamodb_table = args['DYNAMODB_TABLE'] # "MatchesTable"


# Create context and initialise job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read JSON files from S3 into a DataFrame
dataframe = spark.read.option("multiline","true").json(f"s3://{s3_source_bucket}/{s3_source_prefix}")

# Write DataFrame to DynamoDB table
dataframe.write.mode("append") \
    .option("tableName", dynamodb_table) \
    .format("dynamodb") \
    .save()

# commit the job
job.commit()