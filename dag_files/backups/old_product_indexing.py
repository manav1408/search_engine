import configparser
import json
import os
import pickle
import sys

import pymongo
import requests
from datetime import date
from google.cloud import storage
from helper import *

def write(bucket_name, blob_name, text):
    storage_client = storage.Client.from_service_account_json(os.environ.get('GS_SERVICE_ACCOUNT_KEY_FILE'))
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        f.write(text)

def get_products(url, querystring, headers, calls = 5):
    res = []
    total_pages = 0
    offset = int(querystring["offset"])
    complete = False
    
    for call in range(1,calls+1):
        querystring["offset"] = str(offset)
        response = requests.request("GET", url, headers=headers, params=querystring)
        try:
            data = json.loads(response.text)
            n = len(data["products"])
            if n > 0:
                for product in data["products"]:
                    product["categoryId"] = querystring["categoryId"]
                    product["categoryName"] = data["categoryName"]
                    res.append(product)
                offset += n
                if n < 48:
                    print(f"No more products after page {call}!")
                    complete = True
                    break
            else:
                print(f"No more products after page {call}!")
                complete = True
                break
        except:
            print(f"Error encountered at page no {call}")
            print(f"Status code: {response.status_code}")
            print(f"Error message: {json.loads(response.text)}")
            break

        total_pages += 1

    return res, offset, total_pages, complete


def fetch_products():
    
    config = configparser.ConfigParser()
    dirname = os.path.abspath(os.path.dirname(__file__))
    config.read(dirname+"/config.ini")
    url = config["API"]["PRODUCT_URL"]
    
    ip_address = os.environ.get('MONGO_IP')
    username = os.environ.get('MONGO_USERNAME')
    password = os.environ.get('MONGO_PASSWORD')
    database_name = os.environ.get('MONGO_DB_NAME')
    collection_name = "categories"
    # port = 27017
    
    connection_url = f"mongodb+srv://{username}:{password}@{ip_address}"
    
    bucket = os.environ.get('GS_BUCKET_NAME')
    product_filename = "products.txt"

    client = pymongo.MongoClient(connection_url)
    db = client[database_name]
    
    # try:
    #     db.validate_collection(collection_name)
    #     exists = True
    # except pymongo.errors.OperationFailure:
    #     exists = False
    #     print("This collection doesn't exist")
    
    exists = False
    if collection_name in db.list_collection_names():
        print(f"{collection_name} exists!")
        exists = True
        
    if not exists:
        print(f"{collection_name} does not exist!")
        categories_indexing()
     
    collection = db[collection_name]
    category_data = list(collection.find({"complete":False}).limit(5))
    
    headers = {
        "X-RapidAPI-Key": config["API"]["RAPIDAPI_KEY"],
        "X-RapidAPI-Host": config["API"]["RAPIDAPI_HOST"],
        }
    
    products = []
    for category in category_data:
        querystring = {
            "store": "US",
            "offset": category.get("offset",0),
            "categoryId": category["_id"],
            "limit": "48",
            "country": "US",
            "currency": "USD",
            "sizeSchema": "US",
            "lang": "en-US",
        }
        res, offset, pages, complete = get_products(url, querystring, headers)
        products.extend(res)
        try:
            collection.update_one({"_id": category["_id"]}, {"$set":{"offset":offset,"complete":complete}})
        except Exception as e:
            print(e)
    text = json.dumps(products)
    write(bucket,str(date.today())+"/"+product_filename, text)