import pyspark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

'''Spark configuraton setup'''
ss = SparkSession.builder.appName("<YOUR_APP_NAME>").getOrCreate()

mongo_uri =  "<YOUR_MONGO_URI>"
mongo_db = "<YOUR_DB_NAME>"
mongo_collection = "<YOUR_COLLECTION_NAME>"   

'''Fetching and preprocessing data'''
def preprocess_data(sparksession,mongo_uri,mongo_db,mongo_collection):
    raw = (
        sparksession.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("spark.mongodb.input.uri", f"{mongo_uri}/{mongo_db}.{mongo_collection}")
        .load()
    )
    product_names = raw.select('name').rdd.map(lambda x: str(x['name'])).collect()
    return product_names

product_names = preprocess_data(ss,mongo_uri,mongo_db,mongo_collection)

'''Intializing BERT model'''
model_bert = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

'''Computing embeddings for product names using BERT model'''
def get_BERT_embeddings(sentences,model):
    embeddings = model.encode(sentences)
    return embeddings

'''Get top n BERT results for the search query'''
def get_BERT_results(query, n):
    name_embeddings = get_BERT_embeddings(product_names,model_bert)
    query_embeddings = get_BERT_embeddings([query],model_bert)
    similarities = cosine_similarity(query_embeddings, name_embeddings)
    similarities = similarities.flatten()
    indices = (-similarities).argsort()[1:n+1]
    similar_product_names = [product_names[i] for i in indices]
    return similar_product_names

def get_search_results(query, model_type, n=10):
    start_time = time.time()
    if model_type == 'BERT':
        results = get_BERT_results(query, n)
    else:
        print("Invalid model type")
        return

    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds\n")
    print(f"Your search query: {query}")
    for i, pd in enumerate(results):
        print(f"{i}. {pd}")

# Sample queries to test the search
query = "sneakers"
get_search_results(query, "BERT")

query = "t-shirt"
get_search_results(query, "BERT")