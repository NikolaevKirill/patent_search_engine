from datetime import timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time
import pymongo
import redis
from scraper import download_page, parse_patent
import numpy as np
import json

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
            sort=[("patentNumber", pymongo.DESCENDING)],
            projection={"patentNumber": 1, "patent_id": 1},
        )
        if max_patent:
            print(max_patent)
            start_patent = int(max_patent["patentNumber"]) + 1
            start_id = int(max_patent["patent_id"]) + 1
        else:
            start_patent = 2640290
            start_id = 0
        end_patent = start_patent + 10  # 2789828
        for i, number in enumerate(range(start_patent, end_patent)):
            redis_client.rpush(
                "urls_queue",
                json.dumps(
                    (
                        start_id + i,
                        f"https://new.fips.ru/registers-doc-view/fips_servlet?DB=RUPAT&DocNumber={int(number)}&TypeFile=html",
                    )
                ),
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
        patent_id, url = json.loads(redis_client.lpop("urls_queue"))
        page = download_page(url)
        parsed_info = parse_patent(page)
        patent_info = {
            "patent_id": patent_id,
            "patentNumber": parsed_info[0],
            "patentReference": parsed_info[1],
            "patentAbstract": parsed_info[2],
        }
        mongo_collection_patents.insert_one(patent_info)
        time.sleep(3)
    print("После выполнения:")
    print(redis_client.lrange("urls_queue", 0, -1))
    mongo_client.close()


def compute_embeddings_of_docs_and_put_in_faiss():
    mongo_client = pymongo.MongoClient("mongodb://mongodb")
    mongo_db = mongo_client["mydatabase"]
    mongo_collection_patents = mongo_db["patents"]
    all_documents = mongo_collection_patents.find()

    vecs = np.zeros((mongo_collection_patents.estimated_document_count(), 312))
    print(mongo_collection_patents.estimated_document_count())
    model_url = "http://ml_model:81/convert"

    tr_to_ar = lambda x: np.asarray(json.loads(x)).astype(np.float32).reshape(1, -1)

    for i, document in enumerate(all_documents):
        query = {"text": document["patentAbstract"]}
        response = requests.post(model_url, json=query)
        vecs[i] = tr_to_ar(response.json()["vector"])

    faiss_url = "http://faiss_engine:5000/build_index"
    tr = lambda x: json.dumps(x.tolist())
    vecs = {"vectors": tr(vecs)}
    response = requests.post(faiss_url, json=vecs)
    mongo_client.close()


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

op3 = PythonOperator(
    task_id="compute_embedding_and_put",
    python_callable=compute_embeddings_of_docs_and_put_in_faiss,
    dag=dag,
)
op1 >> op2 >> op3
