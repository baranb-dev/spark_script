import os
from pathlib import Path

from watchdog.events import  FileSystemEventHandler, DirCreatedEvent, FileCreatedEvent
import time
from ParseLog import parse_log

class CFileHandler(FileSystemEventHandler):

    def __init__(self,spark_session, log_schema, udf_read_log, udf_asn, udf_country, data_source, result):
        self.spark_session = spark_session
        self.udf_read_log = udf_read_log
        self.udf_asn = udf_asn
        self.udf_country = udf_country
        self.data_source = data_source
        self.result = result
        self.log_schema = log_schema

    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        if not event.is_directory :
            file_detected_full_path = event.src_path
            file_name = file_detected_full_path.split("/")[-1]
            print("File detected", file_name)
            check_file(file_detected_full_path)
            # read a raw file
            df_raw = self.spark_session.read.text(file_detected_full_path)

            start = time.time()
            df_2 = df_raw.rdd.map(lambda x:
                                  parse_log(x['value'])
                                  ).toDF(self.log_schema)

            df_3 = (df_2
                    .withColumn("remote_ASN", self.udf_asn(df_2.remote_host))
                    .withColumn("remote_COUNTRY", self.udf_country(df_2.remote_host)))

            df_3.write.csv(self.result + file_name , mode="overwrite", header="True", emptyValue='')

            end = time.time()
            print("Processing time: " + str(end - start))
            os.remove(file_detected_full_path)


def check_file(filepath):
    '''
    Function to check file size if the copy is complete, this function add at least 1 second delay before the
    treatment of the file make sure it's needed in your case.
    :param filepath: file to check, no check is done in the function make sure the file exist
    :return: True when no size change is detected after one second
    '''
    file = Path(filepath)
    file_size_old = file.stat().st_size
    time.sleep(1)
    while True:
        file_size_new = file.stat().st_size
        if file_size_old == file_size_new :
            return True
        else:
            file_size_old = file_size_new
            time.sleep(1)