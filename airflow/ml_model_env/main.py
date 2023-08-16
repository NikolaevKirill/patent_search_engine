import json
from fastapi import FastAPI
from pydantic import BaseModel
import torch
from transformers import AutoTokenizer, AutoModel

app = FastAPI()

try:
    model = AutoModel.from_pretrained("/app/saved_model")
    tokenizer = AutoTokenizer.from_pretrained("/app/saved_model")
    read = True
except OSError:
    model = AutoModel.from_pretrained("cointegrated/rubert-tiny2")
    tokenizer = AutoTokenizer.from_pretrained("cointegrated/rubert-tiny2")
    read = False


def embed_bert_cls(text, model, tokenizer):
    t = tokenizer(text, padding=True, truncation=True, return_tensors="pt")
    with torch.no_grad():
        model_output = model(**{k: v.to(model.device) for k, v in t.items()})
    embeddings = model_output.last_hidden_state[:, 0, :]
    embeddings = torch.nn.functional.normalize(embeddings)
    return embeddings[0].cpu().numpy()


# routes
class Input(BaseModel):
    text: str


@app.get("/status")
async def hello():
    return {"read": read}


@app.post("/convert")
async def vectorize(inp: Input):
    embeddings = embed_bert_cls(inp.text, model, tokenizer)
    return {"vector": json.dumps(embeddings.tolist())}  # {"vec": vec}
