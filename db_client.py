import os
from openai import OpenAI
#Create connection with vectorDB(Milvus)
from pymilvus import MilvusClient
from tqdm import tqdm
import json
import streamlit as st


OPENAI_API_KEY = st.secrets['OPENAI_API_KEY']
openai_client = OpenAI(api_key=OPENAI_API_KEY)


def emb_text(text):
    return (
        openai_client.embeddings.create(input=text, model="text-embedding-3-small")
        .data[0]
        .embedding
    )

def emb_text_d756(text):
    return (
        openai_client.embeddings.create(input=text, model="text-embedding-3-small", dimensions=756)
        .data[0]
        .embedding
    )

@st.cache_resource
def get_cloud_client():
    CLUSTER_ENDPOINT = st.secrets['CLUSTER_ENDPOINT']
    TOKEN = st.secrets['ZILLIS_API_KEY']

    client = MilvusClient(
        uri=CLUSTER_ENDPOINT,
        token=TOKEN 
    )
    return client

@st.cache_resource
def get_db_client():
    milvus_client = MilvusClient(uri="http://localhost:19530")
    return milvus_client

@st.cache_resource
def create_new_db_client(collection_name):
    milvus_client = MilvusClient(uri="http://localhost:19530")

    DIMENSION = 1536  # Dimension of vector embedding
    if milvus_client.has_collection(collection_name):
        milvus_client.drop_collection(collection_name)

    milvus_client.create_collection(
        collection_name=collection_name,
        dimension=DIMENSION,
        metric_type="COSINE",  # Inner product distance
        consistency_level="Strong",  # Strong consistency level
    )
    return milvus_client

def get_query_embeddings(client,question, collection_name):
    search_res = client.search(
        collection_name=collection_name,
        data=[
            emb_text_d756(question)
        ],  # Use the `emb_text` function to convert the question to an embedding vector
        limit=10,  # Return top 3 results
        search_params={"metric_type": "COSINE", "params": {}},  # Inner product distance
        output_fields=["text"],  # Return the text field
    )
    retrieved_lines_with_distances = [(res["entity"]["text"]) for res in search_res[0]]
    return retrieved_lines_with_distances

def insert_embeddings(client, embeddings, collection_name):
    data = []

    for i,line in enumerate(tqdm(embeddings, desc="Creating embeddings")):
        data.append({"id": i, "vector": line[1], "text": line[0]})
    client.insert(collection_name=collection_name, data=data)

