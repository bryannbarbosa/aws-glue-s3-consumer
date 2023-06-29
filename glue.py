import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

def get_s3_object_source_path():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_trigger_event'])
    s3_event = json.loads(args['s3_trigger_event'])
    bucket = s3_event['s3']['bucket']['name']
    key = s3_event['s3']['object']['key']
    s3_file_location = f's3://{bucket}/{key}'

    return s3_file_location

def get_s3_object_destination_path():
    output_filename = "custom_output_file.json"  # Set the desired custom file name
    return get_s3_object_source_path() + "/" + output_filename

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    dynamicFrame = glueContext.create_dynamic_frame.from_options(
      format_options={"rowTag": "Categorias"},
      connection_type="s3",
      format="xml",
      connection_options={"paths": [get_s3_object_source_path()]},
      transformation_ctx="dynamicFrame",
    )

    dynamicFrame = dynamicFrame.repartition(1)

    glueContext.write_dynamic_frame.from_options(
        frame=dynamicFrame,
        connection_type="s3",
        connection_options={"path": get_s3_object_destination_path(), "partitionKeys": []},
        format="json"
    )

main()
