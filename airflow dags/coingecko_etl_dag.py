from airflow.sdk import dag, task
import requests
import pendulum
import json
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from airflow.models import Variable

now = pendulum.now("Asia/Kolkata")

coin_ids = ["bitcoin", "ethereum", "tether", "binancecoin", "solana",
                "usd-coin", "ripple", "dogecoin", "cardano", "shiba-inu"]

paths = [
        {
            "name": "coins",
            "path": "coins/markets",
            "params": {"vs_currency": "usd", "ids": ",".join(coin_ids)},
            "key": f"coins/year={now.year}/month={now.month:02d}/day={now.day:02d}/coins_data_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        },
        {
            "name": "global",
            "path": "global",
            "params": None,
            "key": f"global_metrics/year={now.year}/month={now.month:02d}/day={now.day:02d}/global_data_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        },
        {
            "name": "exchanges_1",
            "path": "exchanges/binance",
            "params": None,
            "key": f"exchanges/year={now.year}/month={now.month:02d}/day={now.day:02d}/binance_data_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        },
        {
            "name": "exchanges_2",
            "path": "exchanges/kraken",
            "params": None,
            "key": f"exchanges/year={now.year}/month={now.month:02d}/day={now.day:02d}/kraken_data_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        },
        {
            "name": "exchanges_3",
            "path": "exchanges/coindcx",
            "params": None,
            "key": f"exchanges/year={now.year}/month={now.month:02d}/day={now.day:02d}/coindcx_data_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        }
    ]

@dag(
    dag_id="coingecko_etl",
    schedule= "0 10 * * *",
    start_date=pendulum.datetime(2026, 4, 24, tz="Asia/Kolkata"),
    catchup=False
)
def coingecko_etl():

    start =  EmptyOperator(task_id="start_task")
    end = EmptyOperator(task_id="end_task")

    @task
    def call_api(path, params=None):
        headers = {"x-cg-demo-api-key": Variable.get("api_key")}
        api_url = f"https://api.coingecko.com/api/v3/{path}"

        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        return json.dumps(response.json())

 

    fetch_api_task = []
    write_api_task = []
    for item in paths:
        fetch_task = call_api.override(task_id=f"fetch_{item['name']}")(
            path=item["path"],
            params=item["params"],
            )
        fetch_api_task.append(fetch_task)
        write_task = S3CreateObjectOperator(
        task_id=f"write_{item['name']}",
        s3_bucket="coingecko-api-raw",
        s3_key=item["key"],
        data=fetch_task
        )
    
        write_api_task.append(write_task)
        fetch_task >> write_task
    
    glue_crawler_task = GlueCrawlerOperator(
        region_name='us-east-1',
        task_id="run_crawler",
        config={"Name": "coingecko_raw_crawler"}
        )

    glue_job_task_refined = GlueJobOperator(
        region_name='us-east-1',
        task_id="raw_to_refined",
        job_name="coingecko_raw_to_refined",
        )

    glue_job_task_curated = GlueJobOperator(
        region_name='us-east-1',
        task_id="refined_to_curated",
        job_name="coingecko_refined_to_curated",
        )

    start >> fetch_api_task
    write_api_task >> glue_crawler_task >> glue_job_task_refined >> glue_job_task_curated >> end

coingecko_etl()
