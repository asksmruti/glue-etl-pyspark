#########################################
### IMPORT LIBRARIES AND SET VARIABLES
#########################################

# Import python modules
import re
import boto3
import sys
from datetime import datetime, timedelta, date
import logging

# Import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.types import DataType, StructType, ArrayType
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

# Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

#########################################
### MODULE TO FLATTEN ALL NESTED COLUMNS - pip install sparkaid
#########################################

def __rename_nested_field__(in_field: DataType, fieldname_normaliser):
    if isinstance(in_field, ArrayType):
        dtype = ArrayType(__rename_nested_field__(in_field.elementType, fieldname_normaliser), in_field.containsNull)
    elif isinstance(in_field, StructType):
        dtype = StructType()
        for field in in_field.fields:
            dtype.add(fieldname_normaliser(field.name), __rename_nested_field__(field.dataType, fieldname_normaliser))
    else:
        dtype = in_field
    return dtype


def __normalise_fieldname__(raw: str):
    return re.sub('[^A-Za-z0-9_]+', '_', raw.strip())


def __get_fields_info__(dtype: DataType, name: str = ""):
    ret = []
    if isinstance(dtype, StructType):
        for field in dtype.fields:
            for child in __get_fields_info__(field.dataType, field.name):
                wrapped_child = ["{prefix}{suffix}".format(
                    prefix=("" if name == "" else "`{}`.".format(name)), suffix=child[0])] + child[1:]
                ret.append(wrapped_child)
    elif isinstance(dtype, ArrayType) and (
            isinstance(dtype.elementType, ArrayType) or isinstance(dtype.elementType, StructType)):
        for child in __get_fields_info__(dtype.elementType):
            wrapped_child = ["`{}`".format(name)] + child
            ret.append(wrapped_child)
    else:
        return [["`{}`".format(name)]]
    return ret


def normalise_fields_names(df: DataFrame, fieldname_normaliser=__normalise_fieldname__):
    return df.select([
        f.col("`{}`".format(field.name)).cast(__rename_nested_field__(field.dataType, fieldname_normaliser))
            .alias(fieldname_normaliser(field.name)) for field in df.schema.fields
    ])


def flatten(df: DataFrame, fieldname_normaliser=__normalise_fieldname__):
    cols = []
    for child in __get_fields_info__(df.schema):
        if len(child) > 2:
            ex = "x.{}".format(child[-1])
            for seg in child[-2:0:-1]:
                if seg != '``':
                    ex = "transform(x.{outer}, x -> {inner})".format(outer=seg, inner=ex)
            ex = "transform({outer}, x -> {inner})".format(outer=child[0], inner=ex)
        else:
            ex = ".".join(child)
        cols.append(f.expr(ex).alias(fieldname_normaliser("_".join(child).replace('`', ''))))
    return df.select(cols)


#########################################
### IMPORT LIBRARIES AND SET VARIABLES
#########################################

# Assume role
def assume_role(role_arn):
    #args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    #job_run_id = args['JOB_RUN_ID']
    job_run_id = 'j-13290'
    session = boto3.session.Session()
    sts_connection = session.client('sts')
    response = sts_connection.assume_role(
        RoleArn = role_arn,
        RoleSessionName = job_run_id,
        DurationSeconds = 3600)
    credentials = response['Credentials']

    # Set assume role creds
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider',
                                                      'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.access.key', credentials['AccessKeyId'])
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.secret.key', credentials['SecretAccessKey'])
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.session.token', credentials['SessionToken'])
    return credentials


#########################################
### SETTING INPUT PATH
#########################################
def get_input_path(b_name, i_path, days):
    # Bucket name without s3://
    bucket = str(b_name)
    # Input path without bucket name and ends with "/"
    prefix = str(i_path)
    days = int(days)
    old_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    # current_date = datetime.today().strftime("%Y-%m-%d")
    current_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    start = datetime.strptime(old_date, "%Y-%m-%d")
    end = datetime.strptime(current_date, "%Y-%m-%d")
    date_array = \
        (start + timedelta(days=x) for x in range(0, (end - start).days))

    s3_read_path = []
    for date_object in date_array:
        s3_read_path.append(
            str("s3a://" + bucket + "/" + prefix + "partition-date=" + date_object.strftime("%Y-%m-%d") + "/"))

    if len(s3_read_path) <= 0:
        raise Exception("ERROR: Files not found s3://{}/{}/", format(bucket_name, input_path))
    return s3_read_path


#########################################
### EXTRACT (READ DATA)
#########################################
def extract_data(s3_read_path, file_format):
    logging.info("Extracting files...")
    print("S3 Path: ", s3_read_path)
# Read data to Glue dynamic frame
    try:
        dynamic_frame_read = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": s3_read_path, "recurse": True},
            format=file_format,
            format_options={"withHeader": True}
        )
    except ValueError:
        logging.error("Unable to read dynamic frame")

    logging.info("Schema of dynamic dataframe", dynamic_frame_read.printSchema())
    logging.info("Total number of records in dataframe", dynamic_frame_read.count())
    return dynamic_frame_read


#########################################
### TRANSFORM (MODIFY DATA)
#########################################
def transform_data(transform_data_frame):
    logging.info("Transformation started...")
    df_without_null = DropNullFields.apply(frame=transform_data_frame)
# Convert dynamic frame to data frame to use standard pyspark functions
    df = df_without_null.toDF()
    print("DF_WITHOUT_NULL: ", df)
    
# Select a column
    # df.select("orderReference").show()
# Use filter as where clause
    # df.select(df["checkpoints"]).filter(f.col("orderReference") == "RK2XVH3M").show()
# Completely flatten the nested columns (Both Struct & Array)
    # flatten(df).show(1)
# Rename Column
    # df.withColumnRenamed("oldName", "newName")
# Select columns
    # df.select("id","name","time","city")

    #df = flatten(df)
    print("Row count after flattening: ", df.count())
# Actual transformation logic - This will change as per requirement
    '''
    try:
        df = df.withColumn("checkpointTime", f.col("checkpoints_checkpointTime")) \
            .withColumn("location", f.col("checkpoints_location_string")) \
            .withColumn('createdAt', f.col('createdAt').cast('timestamp')) \
            .withColumn("createdDate", f.split(f.col("createdAt"), " ").getItem(0)) \
            .withColumn("createdTime", f.split(f.col("createdAt"), " ").getItem(1)) \
            .withColumn("smruti", lit(datetime.now().strftime("%Y-%m-%d"))) \ ## Apend a new column
            .drop("checkpoints_checkpointTime", "checkpoints_location_string") \
            .dropDuplicates()
    except ValueError:
        logging.error("Unable to transform data")
    '''
# Replace unwanted strings from column names, this maybe useful only when you are flattening all columns
    for name in df.schema.names:
        df = df.withColumnRenamed(name, name.replace('_string', '')) \
               .select(sorted(df.columns))

    logging.info("Row count after deleting duplicates: ", df.count())
    logging.info("Schema of dataframe after transformation: ", df.printSchema())

    return df


#########################################
### LOAD (WRITE DATA)
#########################################
def load_data(load_data_frame, s3_write_path, file_format):
    logging.info("Loading files...")
    df = load_data_frame
# Create just 1 partition, because there is so little data
    data_frame_aggregated = df.repartition(1) 

# Convert back to dynamic frame
    dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")

# Write data back to S3
# glue_context.write_dynamic_frame.from_options(
#    frame = dynamic_frame_write,
#    connection_type = "s3",
#    connection_options = {"path": s3_write_path, "partitionKeys": ["createdDate"]},
#    format = "parquet",
#    format_options = {"withHeader": True}
# )
# GlueContext doesn't provide the option to overwrite, hence convert to DynamicFrame to Spark Dataframe
    try:

        dynamic_frame_write.toDF() \
            .write \
            .mode("overwrite") \
            .format(file_format) \
            .partitionBy("transactionDate") \
            .save(s3_write_path)
    except ValueError:
        logging.error("File loading failed")

    logging.info("Schema of dynamic dataframe", dynamic_frame_write.printSchema())
    logging.info("Total records written: ", dynamic_frame_write.toDF().count())
    logging.info("ETL job completed")



s3_path = ""
read_file_format = "json"
write_file_format = "parquet"
role_arn = ""
bucket_name = ""
input_path = ""
look_back_days = 1

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)

assume_role(role_arn)
e_s3_read_path = get_input_path(bucket_name, input_path, look_back_days)
print(e_s3_read_path)
e_data_frame = extract_data(e_s3_read_path, read_file_format)
print(e_data_frame)
t_data_frame = transform_data(e_data_frame)
print(t_data_frame)
l_s3_write_path = str("s3a://" + s3_path)
print(l_s3_write_path)
load_data(t_data_frame, l_s3_write_path, write_file_format)
