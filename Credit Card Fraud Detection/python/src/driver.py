# Streaming Application to read from Kafka
# This should be the driver file for your project

# Importing all the required function
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Creating Spark Session 
spark = SparkSession  \
        .builder  \
        .appName("CapStone_Project")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
sc = spark.sparkContext

# Adding the required python files
sc.addPyFile('db/dao.py')
sc.addPyFile('db/geo_map.py')
sc.addFile('rules/rules.py')

# Importing all the required files
import dao
import geo_map
import rules

# Reading from Kafka stream
lines = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","transactions-topic-verified")  \
        .option("failOnDataLoss","false").option("startingOffsets","earliest")  \
        .load()

# Defining Schema
schema =  StructType([
                StructField("card_id", StringType()),
                StructField("member_id", StringType()),
                StructField("amount", IntegerType()),
                StructField("pos_id", StringType()),
                StructField("postcode", StringType()),
                StructField("transaction_dt", StringType())
            ])

# Parsing the data
parse = lines.select(from_json(col("value") \
                                    .cast("string") \
                                    ,schema).alias("parsed"))

# Parseing the dataframe
df_parsed = parse.select("parsed.*")

# Adding Time stamp column
df_parsed = df_parsed.withColumn('transaction_dt_ts',unix_timestamp(df_parsed.transaction_dt, 'dd-MM-YYYY HH:mm:ss').cast(TimestampType()))

# Function for Credit Score
def score(a):
    hdao = dao.HBaseDao.get_instance()
    data_fetch = hdao.get_data(key=a, table='look_up_table')
    return data_fetch['info:score']

# Defining UDF for Credit Score
score_udf = udf(score,StringType())

# Adding Score Column
df_parsed = df_parsed.withColumn("score",score_udf(df_parsed.card_id))

# Function for Postal Code
def postcode(a):
    hdao = dao.HBaseDao.get_instance()
    data_fetch = hdao.get_data(key=a, table='look_up_table')
    return data_fetch['info:postcode']

# Defining UDF for Postal Code
postcode_udf = udf(postcode,StringType())

# Adding Postal Code Column
df_parsed = df_parsed.withColumn("last_postcode", postcode_udf(df_parsed.card_id))

# Function for UCL
def ucl(a):
    hdao = dao.HBaseDao.get_instance()
    data_fetch = hdao.get_data(key=a, table='look_up_table')
    return data_fetch['info:UCL']
`
# Defining UDF for UCL
ucl_udf = udf(ucl,StringType())

# Adding UCL Column
df_parsed = df_parsed.withColumn("UCL", ucl_udf(df_parsed.card_id))

# Function for Calculating distance
def dist_cal(last_postcode,postcode):
	gmap = geo_map.GEO_Map.get_instance()
	last_lat = gmap.get_lat(last_postcode)
	last_longg = gmap.get_long(last_postcode)
	lat = gmap.get_lat(postcode)
	longg = gmap.get_long(postcode)
	d = gmap.distance(last_lat.values[0],last_longg.values[0],lat.values[0],longg.values[0])
	return d

# Defining UDF for Distance
distance_udf = udf(dist_cal,DoubleType())

# Adding Distance Column
df_parsed = df_parsed.withColumn("distance",distance_udf(df_parsed.last_postcode,df_parsed.postcode))

# Function for Transaction Date
def Tdate(a):
    hdao = dao.HBaseDao.get_instance()
    data_fetch = hdao.get_data(key=a, table='look_up_table')
    return data_fetch['info:transaction_date']

# Defining UDF for Transaction Date
Tdate_udf = udf(Tdate,StringType())

# Adding Transaction Date Column
df_parsed = df_parsed.withColumn("last_transaction_date",Tdate_udf(df_parsed.card_id))

# Adding Time stamp column
df_parsed = df_parsed.withColumn('last_transaction_date_ts',unix_timestamp(df_parsed.last_transaction_date, 'YYYY-MM-dd HH:mm:ss').cast(TimestampType()))

# Function to Calculate Time
def time_cal(last_date,curr_date):
    d = curr_date - last_date
    return d.total_seconds()

# Defining UDF for Calculating Time
time_udf = udf(time_cal,DoubleType())

# Adding Time diff column
df_parsed = df_parsed.withColumn('time_diff',time_udf(df_parsed.last_transaction_date_ts,df_parsed.transaction_dt_ts))

# Function to define the Status of the Transaction
def status_def(card_id,member_id,amount,pos_id,postcode,transaction_dt,transaction_dt_ts,last_transaction_date_ts,score,distance,time_diff):
    hdao = dao.HBaseDao.get_instance()
    geo = geo_map.GEO_Map.get_instance()
    look_up = hdao.get_data(key=card_id, table='look_up_table')
    status = 'FRAUD'
    if rules.rules_check(data_fetch['info:UCL'],score,distance,time_diff,amount):
        status = 'GENUINE'
        data_fetch['info:transaction_date'] = str(transaction_dt_ts)
        data_fetch['info:postcode'] = str(postcode)
        hdao.write_data(card_id, data_fetch,'look_up_table')
    row = {'info:postcode': bytes(postcode),'info:pos_id': bytes(pos_id),'info:card_id': bytes(card_id),'info:amount': bytes(amount),
           'info:transaction_dt': bytes(transaction_dt),'info:member_id': bytes(member_id), 'info:status': bytes(status)}
    key = '{0}.{1}.{2}.{3}'.format(card_id,member_id,str(transaction_dt),str(datetime.now())).replace(" ","").replace(":", "")
    hdao.write_data(bytes(key),row,'card_transactions')
    return status

# Defining UDF for Status
status_udf = udf(status_find,StringType())

# Adding Status Column
df_parsed = df_parsed.withColumn('status',status_udf(df_parsed.card_id,df_parsed.member_id,df_parsed.amount,df_parsed.pos_id,
                                                     df_parsed.postcode,df_parsed.transaction_dt,df_parsed.transaction_dt_ts,
                                                     df_parsed.last_transaction_date_ts,df_parsed.score,df_parsed.distance,df_parsed.time_diff))

# Displaying only required columns
df_parsed = df_parsed.select("card_id","member_id","amount","pos_id","postcode","transaction_dt_ts","status")

# Printing Output on Console
query1 = df_parsed \
        .writeStream  \
        .outputMode("append")  \
        .format("console")  \
        .option("truncate", "False")  \
        .start()

# Terminating the Query
query1.awaitTermination()