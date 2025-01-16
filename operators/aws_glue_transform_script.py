import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import gs_parse_json
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1736996368406 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://leetcode-contest-analytics/raw/contest_ranking.csv"]}, transformation_ctx="AmazonS3_node1736996368406")

# Script generated for node Change Schema
ChangeSchema_node1736998468651 = ApplyMapping.apply(frame=AmazonS3_node1736996368406, mappings=[("ranking", "string", "ranking", "string"), ("currentrating", "string", "rating", "float"), ("currentglobalranking", "string", "global_ranking", "int"), ("dataregion", "string", "data_region", "string"), ("user", "string", "user", "string")], transformation_ctx="ChangeSchema_node1736998468651")

# Script generated for node Parse JSON Column
ParseJSONColumn_node1736996392282 = ChangeSchema_node1736998468651.gs_parse_json(colName="user")

# Manual transformation
df = ParseJSONColumn_node1736996392282.toDF()

# Process the ranking column
df = df.withColumn("ranking", F.from_json("ranking", "array<int>"))
df = df.withColumn("contest_count", F.size(F.col("ranking")))  # Number of contests attended
df = df.withColumn("avg_ranking", F.expr("aggregate(ranking, 0D, (acc, x) -> acc + x) / size(ranking)"))

# Process the user column
df = df.withColumn("username", F.col("user")["username"])  # Extract username
df = df.withColumn("country", F.col("user")["profile"]["countryName"])  # Extract country

df = df.withColumn("country", F.when(F.col("data_region") == "CN", "China").otherwise(F.col("country")))  # Fill country for CN users
df = df.withColumn("country", F.when(F.col("country") == "", "Unknown").otherwise(F.col("country")))
df = df.fillna({"country": "Unknown"})
df = df.drop("ranking", "user", "data_region")  # Unnecessary columns

# Convert back to DynamicFrame
df = DynamicFrame.fromDF(df, glueContext, "transformed_dynamic_frame")

# Script generated for node Amazon S3
AmazonS3_node1736996407493 = glueContext.write_dynamic_frame.from_options(frame=df, connection_type="s3", format="csv", connection_options={"path": "s3://leetcode-contest-analytics/processed/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1736996407493")

job.commit()