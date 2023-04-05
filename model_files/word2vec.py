from pyspark.ml.feature import HashingTF, IDF, Word2Vec
from pyspark.ml.feature import Normalizer
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import pyspark
from pyspark import SparkContext
from pyspark.ml.feature import (
    IDF,
    HashingTF,
    Normalizer,
    RegexTokenizer,
    StopWordsRemover,
)
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import FloatType

# TODO: Read from os.environment
mongo_uri = "mongodb://localhost:27017"
mongo_db = "msds697"
mongo_collection = "products"

def get_spark_objects():
    conf = (
        pyspark.SparkConf()
        .set(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        )
        .setMaster("local")
        .setAppName("Pre-processing")
    )
    sc = SparkContext(conf=conf)
    sql_c = SQLContext(sc)
    ss = SparkSession.builder.appName("SearchEngine").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    return sc, sql_c, ss

def fetch_data_from_mongodb(sql_c):
    data = (
        sql_c.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("spark.mongodb.input.uri", f"{mongo_uri}/{mongo_db}.{mongo_collection}")
        .load()
    )
    return data

def cosine_similarity(vec1, vec2):
    dot_product = float(vec1.dot(vec2))
    norm_product = float(vec1.norm(2) * vec2.norm(2))
    similarity = dot_product / norm_product if norm_product != 0 else 0.0
    return similarity

def build_word2vec_model(input_data, ss):
    
    tokenizer = RegexTokenizer().setPattern("\\W+").setInputCol("name").setOutputCol("words")
    stop_words = StopWordsRemover.loadDefaultStopWords("english")
    remover = StopWordsRemover(
        inputCol="words", outputCol="filtered_words", stopWords=stop_words
    )
    
    preprocessed_data = tokenizer.transform(input_data)
    preprocessed_data = remover.transform(preprocessed_data)
    
    word2vec = Word2Vec(vectorSize = 100, minCount = 5, inputCol = 'filtered_words', outputCol = 'result')
    model = word2vec.fit(preprocessed_data)
    result = model.transform(preprocessed_data)
    
    return result, model

def build_search_vec(model, search_query, ss):
    
    tokenizer = RegexTokenizer().setPattern("\\W+").setInputCol("query").setOutputCol("words")
    stop_words = StopWordsRemover.loadDefaultStopWords("english")
    remover = StopWordsRemover(
        inputCol="words", outputCol="filtered_words", stopWords=stop_words
    )
    
    search_df = sc.parallelize([(1, search_query)]).toDF(['id','query'])
    
    preprocessed_data = tokenizer.transform(search_df)
    preprocessed_data = remover.transform(preprocessed_data)

    # run word2vec model on input string
    search_query_vec = model.transform(preprocessed_data)
    search_query_vec = search_query_vec.select('result')
  
    return search_query_vec

if __name__ == "__main__":
    search_query = "sneakers"
    sc, sql_c, ss = get_spark_objects()
    data = fetch_data_from_mongodb(sql_c)
    master_data, model = build_word2vec_model(data, ss)
    search_query_vec = build_search_vec(model, search_query, ss).collect()[0][0]
    cosine_similarity_udf2 = udf(lambda x: cosine_similarity(search_query_vec,x), FloatType())
    similarity_df = master_data.withColumn("similarity", cosine_similarity_udf2(master_data["result"]))
    similar_products = similarity_df.sort("similarity", ascending=False).select('name', 'similarity').limit(10).collect()
    print(similar_products)