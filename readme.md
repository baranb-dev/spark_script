# Simple pyspark script

1. Install
2. How to use it ?
3. Troubleshoot ?


## 1. Installation

```bash

git clone https://github.com/baranb-dev/spark_script.git
python -m venv spark_script/.venv
cd spark_script
source bin/activate
pip install -r requirements.txt
```

If you don't want all the Warning message of Spark, copy the conf dir in pyspark lib ( .venv/lib/python3.13/site-packages/pyspark/)

## 2. How to use it 

### ApacheLogToCsv

This script take a source directory and check if a file is created ( important ) then process it.
It will add two extra columns ( IP location country + ASN Name) then save it as csv.

**Configuration**

You'll need maxmind geolite database.

[Maxmind Link](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data/)

| Database | Name                  |
|----------|-----------------------|
| ASN      | GeoLite2-ASN.mmdb     |
| Country  | GeoLite2-Country.mmdb |


For the input, output and database location you need to fill the config.ini

```ini
[DEFAULT]
SourceDir = ./Source/
OutputDir = ./Output/
MaxmindDb = ./db/db20250725/
```

Adapt the log format or what you need in the Common.py

**log_format and log_schema should match !**

```python
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
```
More tweak are available with ParseLog.py.


### Run it

```bash
spark-submit Process.py
```

And add file to the source dir and wait.



# License

It's free software, you can modify, use and ditribute for free. Do not forget to add the original creator in the rights for other projects. If you have made some improvement of this software you can share your code with the community.