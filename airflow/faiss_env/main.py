import json
import numpy as np
from flask import Flask, jsonify, request
import faiss

app = Flask(__name__)

# routes

index_file_path = "/app/saved/my_index.index"
# vectors_file_path = "/usr/saved/my_vectors.bin"


@app.route("/status", methods=["GET"])
def status():
    return {"status": "exist"}


@app.route("/build_index", methods=["POST"])
def build_index():
    inp = request.get_json()
    vectors = np.asarray(json.loads(inp["vectors"])).astype(np.float32)
    print(vectors, vectors.shape, inp)
    dimension = vectors.shape[1]
    index = faiss.IndexFlatL2(dimension)
    index.add(vectors)

    faiss.write_index(index, index_file_path)

    return {"status": "Done"}

    # Save the data vectors to a binary file
    # vectors.tofile(vectors_file_path)


@app.route("/search", methods=["POST"])
def search():
    inp = request.get_json()
    query_vector = (
        (np.asarray(json.loads(inp["query_vector"]))).astype(np.float32).reshape(1, -1)
    )

    k = int(inp["k"])

    index = faiss.read_index(index_file_path)
    # vectors = np.fromfile(vectors_file_path, dtype=np.float32).reshape(-1, query_vector.shape[1])

    distances, indices = index.search(query_vector, k)

    return {
        "indices": json.dumps(indices.tolist()),
        "distances": json.dumps(distances.tolist()),
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
