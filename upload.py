import subprocess
import mysql.connector
import csv
import sys
import os
import boto3
from signal import signal, SIGPIPE, SIG_DFL

signal(SIGPIPE, SIG_DFL)
csv.field_size_limit(sys.maxsize)

session = boto3.Session(
    aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
    aws_secret_access_key=os.environ['AWS_SECRET_KEY'],
)
s3 = session.resource('s3')
mysql_user = 'root'
mysql_host = 'localhost'
mysql_password = os.environ['MYSQL_PASSWORD']
s3_bucket = 'my-bucket'
s3_region = 'us-east-1'

database = os.environ['Database']
table = os.environ['Table']
filename = os.environ['Filename']
dump_file = 'dump.sql'

subprocess.Popen('mysqldump -u {} -h {} -p{} {} {} >> {}'.format(mysql_user,mysql_host, mysql_password, database, table, dump_file), shell=True).wait()


def get_column_names():
    db = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        passwd=mysql_password,
        database=database
    )

    cursor = db.cursor()
    cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}';".format(database, table))

    result = cursor.fetchall()
    columns = ''
    for x in result:
        columns += ''.join(x) + ","

    f = open(filename, 'w')
    print >>f, columns.strip(',')


def is_insert(line):
    return line.startswith('INSERT INTO') or False


def get_values(line):
    return line.partition('` VALUES ')[2]


def values_sanity_check(values):
    assert values
    assert values[0] == '('
    return True


def parse_values(values, outfile):
    latest_row = []

    reader = csv.reader([values], delimiter=',',
                        doublequote=False,
                        escapechar='\\',
                        quotechar="'",
                        strict=True
                        )

    writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
    for reader_row in reader:
        for column in reader_row:
            if len(column) == 0 or column == 'NULL':
                latest_row.append(chr(0))
                continue
            if column[0] == "(":
                new_row = False
                if len(latest_row) > 0:
                    if latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                if new_row:
                    writer.writerow(latest_row)
                    latest_row = []
                if len(latest_row) == 0:
                    column = column[1:]
            latest_row.append(column)
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            writer.writerow(latest_row)


def make_csv(dump_file, output_file):
    try:
        for line in dump_file:
            if is_insert(line):
                values = get_values(line)
                if values_sanity_check(values):
                    parse_values(values, open(output_file, 'a'))
    except KeyboardInterrupt:
        sys.exit(0)


print('Getting column names for {} table'.format(table))
get_column_names()
print('Exporting csv of {} table'.format(table))
make_csv(open(dump_file, 'r'), filename)
print('Uploading {} to S3 bucket {}'.format(filename, s3_bucket))
s3.meta.client.upload_file(filename, s3_bucket, filename)
