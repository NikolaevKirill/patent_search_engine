import requests
import json
import numpy as np
import streamlit as st
import pandas as pd
import pymongo

model_url = "http://ml_model:81/convert"
faiss_url = "http://faiss_engine:5000/search"

title = st.text_input("Write query")

if title:
    st.write("You entered: ", title)
    query = {"text": title}
    response = requests.post(model_url, json=query)
    tr_to_ar = lambda x: np.asarray(json.loads(x)).astype(np.float32).reshape(1, -1)
    query_vec = tr_to_ar(response.json()["vector"])

    tr = lambda x: json.dumps(x.tolist())

    option = st.selectbox("How much patents?", ("5", "10", "25", "50", "150"))
    query_in_faiss = {"query_vector": tr(query_vec), "k": int(option)}
    response = requests.post(faiss_url, json=query_in_faiss)
    print(response)
    response = response.json()
    indices = tr_to_ar(response["indices"])[0]
    distances = tr_to_ar(response["distances"])[0]

    mongo_client = pymongo.MongoClient("mongodb://mongodb")
    mongo_db = mongo_client["mydatabase"]
    mongo_collection_patents = mongo_db["patents"]

    numbers = []

    for elem in indices.tolist():
        patent_number = mongo_collection_patents.find_one(
            {"patent_id": elem},
            projection={"patentNumber": 1},
        )
        numbers.append(int(patent_number["patentNumber"]))

    numbers = np.array(numbers)

    df = pd.DataFrame(
        np.c_[indices, numbers, distances], columns=["index", "№", "distance"]
    )
    st.table(df)
    mongo_client.close()
