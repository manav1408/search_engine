import json
import os
import sys

import pymongo
import pyspark
from datetime import date
from google.cloud import storage

def clean_data(spark_context,bucket_name,blob_name):
    """
    This functions pulls the data from GCS bucket and maps the data into rdd.
    It takes the spark context, bucket name and file path for the GCS as inputs and returns an rdd.
    """
    storage_client = storage.Client.from_service_account_json(os.environ.get('GS_SERVICE_ACCOUNT_KEY_FILE'))
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    clean_rdd = []
    
    try:
        with blob.open("r") as file:
            products_rdd = spark_context.parallelize(json.loads(file.read()),8)

        clean_rdd = products_rdd.map(lambda x: {
                                        "_id":x['id'],
                                        "name":x['name'],
                                        "price":float(x['price']['current']['value']),
                                        "colour":x['colour'],
                                        "brandName":x['brandName'],
                                        "categoryId":x['categoryId'],
                                        "isSellingFast":x['isSellingFast'],
                                        "facetGroups":x['facetGroupings']
                                    })

    except Exception as e:
        print(e)
        
    return clean_rdd

def push_to_mongo(mongo_collection,input_data):
    """
    This function pushes rdd data into mongoDB
    """
    try:
        mongo_collection.insert_many(input_data.collect())
    except Exception as e:
        print(e)

def gcs_to_mongo():
    spark_context = pyspark.SparkContext()
    bucket_name = os.environ.get('GS_BUCKET_NAME')
    blob_name = str(date.today())+"/products.txt" 
    
    username = os.environ.get('MONGO_USERNAME')
    password = os.environ.get('MONGO_PASSWORD')
    ip_address = os.environ.get('MONGO_IP')
    database_name = os.environ.get('MONGO_DB_NAME')
    collection_name = "products"#os.environ.get('MONGO_PRODUCTS_COLLECTION_NAME')
    # port = 27017
    
    connection_url = f"mongodb+srv://{username}:{password}@{ip_address}"
    
    client = pymongo.MongoClient(connection_url)
    db = client[database_name]
    collection = db[collection_name]
    
    input_rdd = clean_data(spark_context,bucket_name,blob_name)
    push_to_mongo(collection,input_rdd)

    