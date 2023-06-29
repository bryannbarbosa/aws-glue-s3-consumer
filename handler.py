import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

def get_s3_object_path():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_trigger_event'])
    s3_event = args['s3_trigger_event']
    bucket = s3_event['s3']['bucket']['name']
    key = s3_event['s3']['object']['key']
    s3_file_location = f's3://{bucket}/{key}'

    return s3_file_location

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    data_frame = spark.read.format("xml").options(rowTag="Categoria").load(get_s3_object_path())
    processed_data = data_frame.select("Nome")
    output_path = "s3://upload/via-varejo-imports/processed-data.parquet"
    processed_data.write.format("xml").mode("overwrite").save(output_path)

    print(f"Processed data saved to: {output_path}")

main()



