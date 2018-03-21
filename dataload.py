import psycopg2
import bz2
import optparse
import logging
import sys
import csv
import boto3


def main():
    parser = optparse.OptionParser(usage="usage: dataload.py [options]", version=" 1.0")
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,format='%(asctime)s %(levelname)-2s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    parser.add_option("--host",
                      action="store",
                      dest="host",
                      default="localhost",
                      help="Hostname of a postgres instance")
    parser.add_option("--port",
                      action="store",
                      dest="port",
                      default="5432",
                      help="Port number of a postgres instance")
    parser.add_option("--database",
                      action="store",
                      dest="database",
                      default="postgres",
                      help="database name of postgres instance")
    parser.add_option("--user",
                      action="store",
                      dest="user",
                      help="Username of postgres database")
    parser.add_option("--password",
                      action="store",
                      dest="password",
                      help="Password for postgres database")
    parser.add_option("--filename",
                      action="store",
                      dest="filename",
                      help="Absolute path of the file to load")
    parser.add_option("--bucket",
                      action="store",
                      dest="bucket",
                      help="S3 Bucket name")

    logging.info('Parsing commandline parameters')
    (options, args) = parser.parse_args()

    logging.info('Connecting to postgres - ' + options.host)
    conn = psycopg2.connect("host=%s port=%s dbname=%s user=%s password=%s" % (options.host, options.port,options.database, options.user,
                                                                               options.password))

    try:
        cursor = conn.cursor()
        logging.info('Creating Tracking Table')
        logging.info('Tracking Table Created')
        cursor.execute("""create table if not exists  tracking (tracking_surro_id serial primary key,year	int,month	int,dayofmonth	int,dayofweek	int,deptime	varchar(10),crsdeptime	varchar(10),arrtime	 varchar(10),crsarrtime	 varchar(10),uniquecarrier	varchar(4),
                            flightnum	int,tailnum	varchar(10),actualelapsedtime	varchar(10),crselapsedtime varchar(10),airtime	varchar(10),arrdelay varchar(10),depdelay	varchar(10),origin	varchar(10),dest	varchar(10),distance	int,
                            taxiin	int,taxiout	int,cancelled	varchar(10),cancellationcode	varchar(10),diverted	varchar(10),carrierdelay	varchar(10),weatherdelay	varchar(10),nasdelay	varchar(10),securitydelay	varchar(10),lateaircraftdelay varchar(10))
                        """)
        cursor.close()

    except Exception as e:
        logging.info('Exception while creating tracking Table %s' % e)
        conn.rollback()
    finally:
        conn.commit()

    try:
        logging.info('Downloading a file from S3')
        s3_downloaded_file='/tmp/s3_dataset.csv.bz'
        download_s3_file(options.bucket,options.filename,s3_downloaded_file)
        logging.info('Extracting .csv.bz2 file in chunks')
        tempfile="/tmp/flight_dataset.csv"
        BZ2__CSV_LineReader(s3_downloaded_file).writefile(tempfile)
        logging.info('Compressed Dataset extracted into the file %s' % tempfile)
        logging.info('Opening the file %s' % tempfile)
        filehandler = open(tempfile, 'r')
        process_file(conn, 'tracking', filehandler)
    except Exception as e:
        logging.error('Exception while processing file %s' % e)
        conn.rollback()
    finally:
        conn.close()


def process_file(conn, table_name, file_object):
    sql_statement = """
    COPY %s(year,month,dayofmonth,dayofweek,deptime,crsdeptime,arrtime,crsarrtime,uniquecarrier,flightnum,tailnum,actualelapsedtime,crselapsedtime,airtime,arrdelay,depdelay,origin,dest,distance,taxiin,taxiout,cancelled,cancellationcode,diverted,carrierdelay,weatherdelay,nasdelay,securitydelay,lateaircraftdelay) FROM STDIN WITH CSV HEADER DELIMITER AS ','
    """
    try:
        cursor = conn.cursor()
        logging.info("Loading tracking table")
        cursor.copy_expert(sql=sql_statement % table_name, file=file_object)
        print("Load to %s completed" % table_name)
        cursor.close()
    except Exception as e:
        logging.error('Exception while loading file %s' % e)
    finally:
        conn.commit()


class BZ2__CSV_LineReader(object):
    def __init__(self, filename, buffer_size=4*1024):
        self.filename = filename
        self.buffer_size = buffer_size

    def readlines(self):
        with open(self.filename, 'rb') as file:
            for row in csv.reader(self._line_reader(file)):
                yield row
    def writefile(self, file):
        with open(file, "w") as file1:
            writes = csv.writer(file1, delimiter=',', quoting=csv.QUOTE_ALL)
            with open(self.filename, 'rb') as file:
                for row in csv.reader(self._line_reader(file)):
                    writes.writerows([row])

    def _line_reader(self, file):
        buffer = ''
        decompressor = bz2.BZ2Decompressor()

        while True:
            bindata = file.read(self.buffer_size)
            if not bindata:  # EOF?
                break  # Note: Could leave an incomplete "line" in the buffer
                # (but there shouldn't be one in an CSV file).

            block = decompressor.decompress(bindata)
            if sys.version_info >= (3,):  # Python 3
                block = block.decode('utf-8')  # Convert bytes to string.

            if block:
                buffer += block

            if '\n' in buffer:
                lines = buffer.splitlines(True)
                if lines:
                    buffer = '' if lines[-1].endswith('\n') else lines.pop()
                    for line in lines:
                        yield line

def download_s3_file(bucket,source_file,target_file):
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).download_file(source_file, target_file)


if __name__ == '__main__':
    main()
