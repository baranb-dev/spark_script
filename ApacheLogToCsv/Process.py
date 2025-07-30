import configparser
import sys

from pyspark.sql import SparkSession

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from watchdog.observers import Observer

from Common import log_schema
from FileHandler import *
from ParseLog import retrieve_asn, retrieve_country


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('config.ini')

    if not config.has_option('DEFAULT', 'MaxmindDb') :
        print("Missing MaxMindDb option in config.ini")
        sys.exit(-1)
    elif not config.has_option('DEFAULT', 'SourceDir') :
        print("Missing SourceDir option in config.ini")
        sys.exit(-1)
    elif not config.has_option('DEFAULT', 'OutputDir') :
        print("Missing OutputDir option in config.ini")
        sys.exit(-1)


    print("Processing SOURCE: " + config['DEFAULT']['SourceDir'])
    print("Processing OUTPUT: " + config['DEFAULT']['OutputDir'])
    spark = (SparkSession
             .builder
             .appName("Process")
             .getOrCreate())


    # define the udf
    udf_read_log = udf(parse_log, log_schema)
    udf_asn = udf(retrieve_asn,StringType())
    udf_country = udf(retrieve_country,StringType())


    event_handler = CFileHandler(
        spark,
        log_schema,
        udf_read_log,
        udf_asn,
        udf_country,
        config['DEFAULT']['SourceDir'],
        config['DEFAULT']['OutputDir']
    )

    observer = Observer()
    observer.schedule(event_handler,config['DEFAULT']['SourceDir'], recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()