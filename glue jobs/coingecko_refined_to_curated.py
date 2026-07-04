import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


dyf_coins = glueContext.create_dynamic_frame.from_catalog(
    database = "coingecko_refined_db",
    table_name = "coins",
    transformation_ctx = "refined_to_curated_coins"
)

dyf_exchanges = glueContext.create_dynamic_frame.from_catalog(
    database = "coingecko_refined_db",
    table_name = "exchanges",
    transformation_ctx = "refined_to_curated_exchanges"
)

dyf_global_metrics = glueContext.create_dynamic_frame.from_catalog(
    database = "coingecko_refined_db",
    table_name = "global_metrics",
    transformation_ctx = "refined_to_curated_global_metrics"
)

cleaned_dyf_coins = dyf_coins.drop_fields(["year", "month", "day"])
cleaned_dyf_exchanges = dyf_exchanges.drop_fields(["year", "month", "day"])
cleaned_dyf_global_metrics = dyf_global_metrics.drop_fields(["year", "month", "day"])


glueContext.write_dynamic_frame.from_options(
    frame = cleaned_dyf_coins,
    connection_type = "redshift",
    connection_options = {
        "useConnectionProperties": "true",
        "connectionName": "Redshift connection",
        "dbtable": "coingecko_curated.coins",
        "redshiftTmpDir": "s3://coingecko-glue-assets/temporary/"
    },
        transformation_ctx = "write_to_redshift_coins"
    
)

glueContext.write_dynamic_frame.from_options(
    frame = cleaned_dyf_exchanges,
    connection_type = "redshift",
    connection_options = {
        "useConnectionProperties": "true",
        "connectionName": "Redshift connection",
        "dbtable": "coingecko_curated.exchanges",
        "redshiftTmpDir": "s3://coingecko-glue-assets/temporary/"
    },
        transformation_ctx = "write_to_redshift_exchanges"
    
)

glueContext.write_dynamic_frame.from_options(
    frame = cleaned_dyf_global_metrics,
    connection_type = "redshift",
    connection_options = {
        "useConnectionProperties": "true",
        "connectionName": "Redshift connection",
        "dbtable": "coingecko_curated.global_metrics",
        "redshiftTmpDir": "s3://coingecko-glue-assets/temporary/"
    },
        transformation_ctx = "write_to_redshift_global_metrics"
    
)

job.commit()
