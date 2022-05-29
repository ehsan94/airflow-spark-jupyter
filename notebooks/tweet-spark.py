from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob

from datetime import datetime
import pytz

def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words

## Converting date string format
def getDate(x):
    if x is not None:
        return str(datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"))
    else:
        return None

if __name__ == "__main__":
    # create Spark session

    spark = SparkSession.builder.appName("twiiternew").getOrCreate()

    # read the tweet data from socket
    lines = spark.readStream.format("socket").option("host", "0.0.0.0").option("port", 5659).load()
    # # Preprocess the data
    # words = preprocessing(lines)
    # # text classification to define polarity and subjectivity
    # words = text_classification(words)

    structureSchema = StructType([
            StructField('user', StructType([
                StructField('screen_name', StringType(), True),
                StructField('description', StringType(), True),
                StructField('location', StringType(), True),
                StructField('friends_count', StringType(), True),
                StructField('followers_count', StringType(), True),
                StructField('statuses_count', StringType(), True),
                # StructField('created_at', StringType(), True)
                ])),
             StructField('entities', StructType([
                StructField('hashtags', ArrayType(StringType()), True)
             ])),
            StructField('retweeted_status', StructType([
                StructField('extended_tweet', StructType([
                    StructField('full_text', StringType(), True)
                ]))
             ])),
            StructField('text', StringType(), True),
            StructField('created_at', StringType(), True),
            StructField('retweet_count', StringType(), True)
            ])

    schema = StructType().add('text', StringType(), False).add('created_at', StringType(), False).add('id_str', StringType(), False).add('id_str', StringType(), False)
    df = lines.selectExpr('CAST(value AS STRING)').select(from_json('value', structureSchema).alias('temp')).select('temp.*')
    
    df.printSchema()

    # df2 = df.select(col('user.*'), col('entities.*'), col('created_at'), col('retweet_count'))
    df2 = df.select(col('created_at'), col('text'), col('retweeted_status.extended_tweet.*'), col('entities.*'), col('retweet_count'), col('user.*'))
    
    ## UDF declaration
    date_fn = udf(getDate, StringType())

    df2 = df2.withColumn("created_at", to_utc_timestamp(date_fn("created_at"),"UTC"))

    df2.printSchema()

    df2 = df2.repartition(1)

    query = df2.writeStream.queryName("all_tweets_new")\
        .outputMode("append").format("parquet")\
        .option("path", "./parc")\
        .option("checkpointLocation", "./check")\
        .trigger(processingTime='60 seconds').start()

    query.awaitTermination()

    # To print on console
    # query = df2.writeStream.format('console').option('truncate', 'False').start()
    # import time
    # time.sleep(30)
    # query.stop()