import pymysql # connect to mysql database
import csv # output the result of the extraction process to a csv file
import boto3 # communicate with Amazon S3 Bucket to save the extracted file
import configparser # read configuration file so as to as access credentials for mysql and S3 Bucket

# initialize a connection to the MySQL database
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")


conn = pymysql.connect(host=hostname,user=username,password=password,db=dbname,port=int(port))

# Test MySQL connection
if conn is None:
    print("Error connecting to the MySQL database")
else:
    print("MySQL connection established!")

# Extracting all the employees data from the employees database. 

emp_query = "SELECT * FROM employees;"
local_filename = "employees_extract.csv"


# set the MySQL cursor so we can talk to the database with python
m_cursor = conn.cursor()

m_cursor.execute(emp_query)

results = m_cursor.fetchall()

# write 'w' the output to a csv file and name it employees_extract
with open(local_filename, 'w') as f:
    emp_csv_w = csv.writer(f, delimiter='|') # you can change the delimeter to whatever you feel like
    emp_csv_w.writerows(results)
    f.close()
    m_cursor.close()
    conn.close()


# loading the extrated data into S3 Bucket
# load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials","access_key")
secret_key = parser.get("aws_boto_credentials","secret_key")
bucket_name = parser.get("aws_boto_credentials","bucket_name")

# set the AWS Connection
s3 = boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)


"""
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
"""
s3_file = local_filename
s3.upload_file(local_filename,bucket_name,s3_file)
print(f'{local_filename} uploaded succefully')