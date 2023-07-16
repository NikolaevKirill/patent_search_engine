from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time
import pymongo
import redis
from scraper import download_page, parse_patent

default_args = {
    "start_date": days_ago(0, 0, 0, 0, 0),
}

dag = DAG(
    "scraper_dag",
    default_args=default_args,
    description="Scrape patent data and save to MongoDB",
    schedule_interval=timedelta(days=30),
)


def check_redis_queue():
    redis_client = redis.Redis(host="redis_use", port=6379)
    if not redis_client.llen("urls_queue"):
        # mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        mongo_client = pymongo.MongoClient("mongodb://mongodb")
        mongo_db = mongo_client["mydatabase"]
        mongo_collection_patents = mongo_db["patents"]

        max_patent = mongo_collection_patents.find_one(
            sort=[("patentNumber", pymongo.DESCENDING)], projection={"patentNumber": 1}
        )
        if max_patent:
            start_patent = int(max_patent["patentNumber"]) + 1
        else:
            start_patent = 2640290
        end_patent = start_patent + 10 + 1  # 2789828
        for number in range(start_patent, end_patent):
            redis_client.rpush(
                "urls_queue",
                f"https://new.fips.ru/registers-doc-view/fips_servlet?DB=RUPAT&DocNumber={number}&TypeFile=html",
            )
        mongo_client.close()
        print(
            f"Первый в очереди патент: {start_patent}, последний в очереди патент: {end_patent - 1}."
        )
    else:
        print(redis_client.lrange("urls_queue", 0, -1))


def scrape_and_save_to_mongo():
    mongo_client = pymongo.MongoClient("mongodb://mongodb")
    mongo_db = mongo_client["mydatabase"]
    mongo_collection_patents = mongo_db["patents"]
    redis_client = redis.Redis(host="redis_use", port=6379)
    print(redis_client.lrange("urls_queue", 0, -1))
    len_queue = redis_client.llen("urls_queue")
    print(f"Всего патентов в очереди: {len_queue}")
    while redis_client.llen("urls_queue") > 0:
        url = redis_client.lpop("urls_queue")
        page = download_page(url)
        parsed_info = parse_patent(page)
        patent_info = {
            "patentNumber": parsed_info[0],
            "patentReference": parsed_info[1],
            "patentAbstract": parsed_info[2],
        }
        mongo_collection_patents.insert_one(patent_info)
        time.sleep(3)
    print("После выполнения:")
    print(redis_client.lrange("urls_queue", 0, -1))
    # mongo_client.close()


op1 = PythonOperator(
    task_id="urls_queue",
    python_callable=check_redis_queue,
    dag=dag,
)

op2 = PythonOperator(
    task_id="scrape_and_save",
    python_callable=scrape_and_save_to_mongo,
    dag=dag,
)

op1 >> op2
