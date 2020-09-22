#import configparser
#from datetime import datetime
import json
import os

import snowflake.connector
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def set_up_env():
    '''
    function: to set credentials for AWS
    '''

    config = json.load(open('AWS.json'))

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    function: instantiate spark session
    '''

    spark = SparkSession\
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .config("spark.jars","/Users/sanketchobe/Downloads/spark-3.0.1-bin-hadoop2.7/jars/snowflake-jdbc-3.12.8.jar,/Users/sanketchobe/Downloads/spark-3.0.1-bin-hadoop2.7/jars/spark-snowflake_2.12-2.8.1-spark_3.0.jar") \
        .getOrCreate()

    return spark


def data_Extract(spark, data_source, data,error_log_path):
    '''
        Extract the data from multiple sources and have following components..
        Date: Date of the log data
        Hour: Hour at which the log were captured
        File_path: Path of the file name for log data
    '''
    if data_source =='csv':
        if 'date' in data:
            date = str(data['date'])
        else:
            date = ''

        if 'time' in data:
            hour = str(data['time'])
        else:
            hour = '00'

        path =data['path']
        csv_schema =StructType([
                    StructField("v_id", StringType()),
                    StructField("fn_id", StringType()),
                    StructField("status", StringType()),
                    StructField("time", FloatType())
                    ])
        try:
            df = spark.read.csv(path, header=True, nullValue='NA', schema=csv_schema)
            df.show(5)
            df.printSchema()
        except Exception as excpt:
            error_log_path.write(str(excpt))

        return (date, hour, df)
    else:
        error_log_path.write("Unsupported source data type, please check the correct format..")

    '''
        filler code for additional data sources like parquet, json and DB tables
    if data_source =='parquet':
    if data_source =='json'
    '''



def data_Transform(spark, extract_data):
    '''
        Transform the data from multiple sources and create LR dataframe for following tables..
        Drive_Log: current lod data
        Drive_log_Daily: log for the current date
        Drive_Hotspots: Drive hotspots in the current log data
    '''
    date, hour,df = extract_data[0], extract_data[1], extract_data[2]
    df.createOrReplaceTempView("extract_data")

    lr_df1 = spark.sql("""select v_id,
                                fn_id,
                                status,
                                case 
                                    when '{0}'  ='' then current_date() 
                                    else to_date('{0}')
                                end as date,   
                                cast({1} as string) as hour, 
                                time,
                                from_unixtime(cast(time*60 as string), 'mm:ss.SSSSSS') as minutes
                           from extract_data""".format(date, hour))

    lr_df1.createOrReplaceTempView("curr_tb")

    lr_df2 = spark.sql("""select v_id,
                                fn_id,
                                status,
                                to_timestamp(concat(cast(date as string), ' ', cast(hour as string),':', 
                                 cast(minutes as string))) as execution_timestmp
                           from curr_tb
                           where date = current_date()""".format(date, hour))

    lr_df3 = spark.sql("""select v_id,
                                 fn_id,
                                 status,
                                 date,   
                                 hour, 
                                 cast(time as float) as time, 
                                 to_timestamp(concat(cast(date as string), ' ', cast(hour as string),':', 
                                 cast(minutes as string))) as execution_timestmp,
                                 rank() OVER (Partition by v_id, fn_id, status order by time desc) as log_rank 
                            from curr_tb 
                        """.format(date, hour))

    lr_df3.createOrReplaceTempView("log_hotspots")

    lr_df4 = spark.sql("""select a.v_id,
                                 a.fn_id,
                                 abs(a.time - b.time) as execution_time,
                                 a.execution_timestmp
                            from log_hotspots a,
                                 log_hotspots b
                            where a.v_id =b.v_id
                              and a.fn_id = b.fn_id
                              and a.log_rank = b.log_rank
                              and a.status ='start'
                              and b.status ='end'
                       """)

    return (lr_df1, lr_df2, lr_df4)

def create_database(snow_connection, error_log_path):
    '''
    Create the datawarehouse and tables..
    Drive_Analysis_WH : Datawarehouse
    Drive_DB: Database under Datawarehouse
    Drive_USE_Log: Schema under datawarehouse and database
    Vehicle: Dimension table for vehicle details
    Function: Dimension table for function details
    Status: Dimension table for function status details
    Log_Time: Dimension table for log time
    Drive_Log: Fact table for drive log details
    Drive_Log_Daily: Table/View containing the information about daily log captured
    Drive_Hotspots: Table/View for hotspots captured for current log data
    '''

    snow_connection.cursor().execute("use role ACCOUNTADMIN")
    snow_connection.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS Drive_Analysis_WH")
    snow_connection.cursor().execute("CREATE DATABASE IF NOT EXISTS Drive_DB")
    snow_connection.cursor().execute("USE DATABASE Drive_DB")
    snow_connection.cursor().execute("CREATE SCHEMA IF NOT EXISTS Drive_USE_Log")

    snow_connection.cursor().execute("USE DATABASE Drive_DB")
    snow_connection.cursor().execute("USE WAREHOUSE Drive_Analysis_WH")
    snow_connection.cursor().execute("USE SCHEMA Drive_DB.Drive_USE_Log")

    try:
        snow_connection.cursor().execute("CREATE TABLE IF NOT EXISTS "
                                         "Vehicle( vehicle_id varchar(50), "
                                         "vehicle_name varchar(50),"
                                         "vehicle_type varchar(50))"
                                         )
    except Exception as excpt:
        error_log_path.write(str(excpt))
        print(str(excpt))

    try:
        snow_connection.cursor().execute("CREATE TABLE IF NOT EXISTS "
                                         "Function(function_id varchar(50),"
                                         "function_name varchar(50),"
                                         "function_type varchar(50))"
                                         )
    except Exception as excpt:
        error_log_path.write(excpt)
        print("error in Function table creation")

    try:
        snow_connection.cursor().execute("CREATE TABLE IF NOT EXISTS "
                                         "Status(status varchar(10),"
                                         "status_desc varchar(50))"
                                         )
    except Exception as excpt:
        error_log_path.write(str(excpt))
        print(str(excpt))

    try:
        snow_connection.cursor().execute("CREATE TABLE IF NOT EXISTS "
                                         "Log_Time(log_date DATE,"
                                         "log_month varchar(2),"
                                         "log_day varchar(2),"
                                         "log_hour varchar(2),"
                                         "log_timestamp timestamp)"
                                         )
    except Exception as excpt:
        error_log_path.write(str(excpt))
        print(str(excpt))

    try:
        snow_connection.cursor().execute("CREATE TABLE IF NOT EXISTS "
                                         "Drive_Log( vehicle_Id varchar(30),"
                                         "function_Id varchar(30), "
                                         "status varchar(10),"
                                         "log_date DATE,"
                                         "log_hour varchar(2),"
                                         "log_time varchar(30),"
                                         "log_minutes varchar(30))"
                                         )
    except Exception as excpt:
        error_log_path.write(str(excpt))
        print(str(excpt))

    try:
        snow_connection.cursor().execute("CREATE TABLE IF NOT EXISTS "
                                         "Drive_Log_Daily(vehicle_Id varchar(30),"
                                         "function_Id varchar(30),"
                                         "status varchar(10),"
                                         "log_timestamp timestamp)"
                                         )
    except Exception as excpt:
        error_log_path.write(str(excpt))
        print(str(excpt))

    try:
        snow_connection.cursor().execute("CREATE TABLE IF NOT EXISTS "
                                         "Drive_Hotspots(vehicle_Id varchar(30),"
                                         "function_Id varchar(30),"
                                         "execution_time varchar(30),"
                                         "log_timestamp timestamp)"
                                         )
    except Exception as excpt:
        error_log_path.write(str(excpt))
        print(str(excpt))


def load_tables(snow_connect,spark, full_tb, curr_tb, hotspot_tb, error_log_path):
    '''
    Load the data in the following snowflake tables based on the spark-snowflake connection
    Drive_Log: Fact table for drive log details
    Drive_Log_Daily: Table/View containing the information about daily log captured
    Drive_Hotspots: Table/View for hotspots captured for current log data
    '''

    spark_conf = SparkConf().setMaster('local').setAppName('Drive_Log')
    snowflake_source = "net.snowflake.spark.snowflake"

    sfOptions = {
        "sfURL": "zi71392.us-east-2.aws.snowflakecomputing.com",
        "sfUser": snow_connect["userid"],
        "sfPassword": snow_connect["password"],
        "sfDatabase": "Drive_DB",
        "sfSchema": "Drive_USE_Log",
        "sfWarehouse": "Drive_Analysis_WH",
        "sfRole":"ACCOUNTADMIN"
    }


    full_tb.show(5)
    curr_tb.show(5)
    hotspot_tb.show(5)
    try:
        full_tb.write.format(snowflake_source)\
            .options(**sfOptions)\
            .option("dbtable", "Drive_Log")\
            .mode("append").save()
    except Exception as excpt:
        error_log_path.write(str(excpt))

    try:
        curr_tb.write.format(snowflake_source)\
            .options(**sfOptions)\
            .option("dbtable", "Drive_Log_Daily")\
            .mode("append").save()
    except Exception as excpt:
        error_log_path.write(str(excpt))

    try:
        hotspot_tb.write.format(snowflake_source)\
            .options(**sfOptions)\
            .option("dbtable", "Drive_Hotspots")\
            .mode("append").save()
    except Exception as excpt:
        error_log_path.write(str(excpt))


def data_Load(spark, full_tb, curr_tb, hotspot_tb,error_log_path):
    snow_connect = json.load(open('Configs/snow_connect.json'))
    snow_connection = snowflake.connector.connect(
        user=snow_connect["userid"],
        password=snow_connect["password"],
        account=snow_connect["account"],
        session_parameters={
            "QUERY_TAG": "DriveLogWH",
        }
    )

    create_database(snow_connection, error_log_path)
    load_tables(snow_connect,spark, full_tb, curr_tb, hotspot_tb, error_log_path )


def main():
    #set_up_env()
    spark_session = create_spark_session()

    etl_data = json.load(open('Configs/data_source.json'))
    error_log = json.load(open('Configs/error_log.json'))
    error_log_path = open(error_log["error_log"],"a")

    for dataSource, dataSet in etl_data['data_sources'].items():
        print('data source:', dataSource)
        print('data:',dataSet)
        extract_data = data_Extract(spark_session, dataSource, dataSet,error_log_path)
        full_tb, curr_tb, hotspot_tb= data_Transform(spark_session, extract_data)
        print(full_tb, curr_tb, hotspot_tb)
        data_Load(spark_session, full_tb, curr_tb, hotspot_tb,error_log_path)
        error_log_path.close()


if __name__ == '__main__':
    main()