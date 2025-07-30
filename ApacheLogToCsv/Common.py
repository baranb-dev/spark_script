import geoip2
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType

# Define the log format to parse and the log schema for spark here

log_format = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" %T"
log_schema = StructType([
    StructField("remote_host", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("request", StringType(), False),
    StructField("return_status", IntegerType(), False),
    StructField("bytes_sent", LongType(), False),
    StructField("referer", StringType(), False),
    StructField("user_agent", StringType(), False),
])


import configparser
import geoip2.database
import geoip2.errors

# prepare maxminddb
config = configparser.ConfigParser()
config.read('config.ini')

asn_db = geoip2.database.Reader(config['DEFAULT']['MaxmindDb']+"GeoLite2-ASN.mmdb")
country_db = geoip2.database.Reader(config['DEFAULT']['MaxmindDb']+"GeoLite2-Country.mmdb")