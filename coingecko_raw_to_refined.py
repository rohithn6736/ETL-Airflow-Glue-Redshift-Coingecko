import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime
from awsgluedq.transforms import EvaluateDataQuality



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dyf_coins = glueContext.create_dynamic_frame.from_catalog(
     database="coingecko_raw_db",
     table_name="coins",
     transformation_ctx="source_data_coins")

dyf_global_metrics = glueContext.create_dynamic_frame.from_catalog(
     database="coingecko_raw_db",
     table_name="global_metrics",
     transformation_ctx="source_data_global_metrics")
     
dyf_exchanges = glueContext.create_dynamic_frame.from_catalog(
     database="coingecko_raw_db",
     table_name="exchanges",
     transformation_ctx="source_data_exchanges")
     
dyf_coins = dyf_coins.resolveChoice(choice = "match_catalog",database="coingecko_raw_db",table_name="coins")
dyf_global_metrics = dyf_global_metrics.resolveChoice(choice = "match_catalog",database="coingecko_raw_db",table_name    ="global_metrics")
dyf_exchanges = dyf_exchanges.resolveChoice(choice = "match_catalog",database="coingecko_raw_db",table_name="exchanges")



df_coins = dyf_coins.toDF()
df_global_metrics = dyf_global_metrics.toDF()
df_exchanges = dyf_exchanges.toDF()

df_coins = df_coins.select("id","symbol","name","current_price","last_updated","price_change_24h","price_change_percentage_24h",
                           "total_volume","market_cap","market_cap_rank","ath","circulating_supply","total_supply","max_supply",
                           "year","month","day")
df_coins = df_coins.withColumn("last_updated", F.to_timestamp("last_updated", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

df_global_metrics = df_global_metrics.select("data.active_cryptocurrencies","data.total_volume.usd",
                    "data.market_cap_percentage.btc","data.market_cap_change_percentage_24h_usd","data.volume_change_percentage_24h_usd","data.updated_at","year","month","day")
df_global_metrics = df_global_metrics.withColumn("updated_at", F.from_unixtime("updated_at").cast("timestamp"))
df_exchanges = df_exchanges.select("name","country","trust_score","trust_score_rank","trade_volume_24h_btc","year","month","day")
df_exchanges = df_exchanges.withColumn('date_updated', F.make_date('year', 'month', 'day'))


dyf_coins = DynamicFrame.fromDF(df_coins, glueContext, "convert")
dyf_global_metrics = DynamicFrame.fromDF(df_global_metrics, glueContext, "convert")
dyf_exchanges = DynamicFrame.fromDF(df_exchanges, glueContext, "convert")

table_configs = [
    {
        "table_name": "coins",
        "frame": dyf_coins,
        "ruleset": """
            Rules = [
                IsComplete "symbol",
                IsComplete "id",
                IsComplete "current_price",
                IsComplete "last_updated",
                ColumnValues "current_price" >= 0,
                ColumnValues "market_cap" >= 0,
                ColumnValues "total_volume" >= 0,
                ColumnValues "market_cap_rank" > 0,
                ColumnValues "price_change_percentage_24h" >= -100
                ]
        """
    },
    {
        "table_name": "global_metrics",
        "frame": dyf_global_metrics,
        "ruleset": """
            Rules = [
                IsComplete "updated_at",
                ColumnValues "active_cryptocurrencies" > 0
            ]
        """
    },
    {
        "table_name": "exchanges",
        "frame": dyf_exchanges,
        "ruleset": """
            Rules = [
                IsComplete "name",
                ColumnValues "trust_score" >= 0
            ]
        """
    }
]


for config in table_configs:
    table_name = config["table_name"]
    ruleset= config["ruleset"]
    
    dq_results = EvaluateDataQuality().process_rows(
        frame=config["frame"],
        ruleset=ruleset,
        publishing_options={
            "dataQualityEvaluationContext": f"EvaluateDataQuality_{table_name}",
            "enableDataQualityResultsPublishing": True,
            "resultsS3Prefix": f"s3://coingecko-glue-assets/data-quality-results/{table_name}/"
            }
    )
    assert dq_results[EvaluateDataQuality.DATA_QUALITY_RULE_OUTCOMES_KEY]\
    .filter(lambda x: x["Outcome"] == "Failed")\
    .count() == 0, f"The job failed due to failing Data Quality rules for table: {table_name}"



sink_coins = glueContext.getSink(
    path="s3://coingecko-api-refined/coins",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year","month","day"],
    enableUpdateCatalog=True,
    transformation_ctx="datasink_coins"
)
sink_coins.setCatalogInfo(
    catalogDatabase="coingecko_refined_db",
    catalogTableName="coins"
)
sink_coins.setFormat("parquet",useGlueParquetWriter=True)
sink_coins.writeFrame(dyf_coins)

sink_global_metrics = glueContext.getSink(
    path="s3://coingecko-api-refined/global_metrics",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year","month","day"],
    enableUpdateCatalog=True,
    transformation_ctx="datasink_global_metrics"
)
sink_global_metrics.setCatalogInfo(
    catalogDatabase="coingecko_refined_db",
    catalogTableName="global_metrics"
)
sink_global_metrics.setFormat("parquet",useGlueParquetWriter=True)
sink_global_metrics.writeFrame(dyf_global_metrics)

sink_exchanges = glueContext.getSink(
    path="s3://coingecko-api-refined/exchanges",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year","month","day"],
    enableUpdateCatalog=True,
    transformation_ctx="datasink_exchanges"
)
sink_exchanges.setCatalogInfo(
    catalogDatabase="coingecko_refined_db",
    catalogTableName="exchanges"
)
sink_exchanges.setFormat("parquet",useGlueParquetWriter=True)
sink_exchanges.writeFrame(dyf_exchanges)

job.commit()
