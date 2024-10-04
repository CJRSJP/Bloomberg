# File handling here is slightly janky since the repo has 28k files, but seems performant thus far.
# Ideally would split the errorlogs into multiple folders so we don't have to iterate all.

import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import fnmatch
import io
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
bucket_name = 'investmentdatahub-prd'
bucket = s3_resource.Bucket('investmentdatahub-prd')
s3_data = s3_client.get_object(Bucket=bucket_name, Key=env_config_key)
config_contents = s3_data['Body'].read()
config_data = str(config_contents)[:].replace('\\r\\n','').replace('b\'','').replace('\'','')
config= json.loads(config_data)


def list_s3_files(bucket_name, prefix='', keywords=[]):
    '''
    params:
        prefix: here is the file path within the bucket_name, stored as variable in Matillion
        keywords: keyword is used to check for a pattern within the file name
    returns:
        returns a file list as a list of dictionaries
    '''
    continuation_token = None
    all_files = []
    last_week = datetime.now() - timedelta(days=2)

    while True:

        list_params = {
            'Bucket': bucket_name
            ,'Prefix': prefix
            ,'MaxKeys': 1000
        }
        
        if continuation_token:
            list_params['ContinuationToken'] = continuation_token

        response = s3_client.list_objects_v2(**list_params)

        if 'Contents' not in response:
            break
        
        all_files.extend(response['Contents'])

        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    if keywords:
        all_files = [file for file in all_files if any(keyword in file['Key'].replace('Extracts/IDH003/ErrorLogs/', '') for keyword in keywords)]

    # Removing tzinfo since S3 LastModified includes data for timezone - remove timezone awareness
    recent_files = [file for file in all_files if file['LastModified'].replace(tzinfo=None) >= last_week]

    files_sorted = sorted(recent_files, key=lambda x: x['LastModified'], reverse=True)
    
    file_list = [{'Key': file['Key'], 'LastModified': file['LastModified']} for file in files_sorted]

    return file_list

def process_file_errors(bucket_name, file_list, cs):
    '''
    params:
        file_list: where each file is named with 'Key'
    returns:
        a df from the input file(s) where each row contains the word error (case insensitive)
    '''
    error_records = []

    for file in file_list:
        key = file['Key']
        date = file['LastModified']

        cs.execute(f"SELECT COUNT(*) FROM IM_REC.CR_EPHEMERAL.PRICE_ERRORS WHERE FILE_NAME = %s", (key.replace('Extracts/IDH003/ErrorLogs/', ''),))
        file_exists = cs.fetchone()[0] > 0

        if file_exists:
            print(f"File {key} already loaded. Continue...")
            continue

        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            file_data = response['Body'].read().decode('utf-8')

            file_df = pd.read_csv(io.StringIO(file_data), names=['ID', 'STATUS', 'LINE_NUMBER', 'ERROR_MSG'])

            for _, row in file_df.iterrows():
                if row['STATUS'].strip().upper() == 'ERROR':
                    error_records.append({
                            'FILE_NAME': key.replace('Extracts/IDH003/ErrorLogs/', ''),
                            'ID': row['ID'],
                            'STATUS': row['STATUS'],
                            'LINE_NUMBER': row['LINE_NUMBER'],
                            'ERROR_MSG': row['ERROR_MSG'],
                            'LOAD_DATE_TIME': date
                        })
  
        except ClientError as emsg:
            print(f"Failed to retrieve {key} from S3: {emsg}")
        except Exception as emsg:
            print(f"Error processing file {key}: {emsg}")

    error_df = pd.DataFrame(error_records, columns=['FILE_NAME', 'ID', 'STATUS', 'LINE_NUMBER', 'ERROR_MSG', 'LOAD_DATE_TIME'])

    return error_df

ctx = snowflake.connector.connect(
    user = config['user'],
    password = config['password'],
    account= config['account'],
    warehouse = config['warehouse'],
    database = 'IM_REC',
    schema  = 'CR_EPHEMERAL',
    role = config['role'],
    #autocommit = False,
    )
cs = ctx.cursor()

keywords = ['IDH003_PRICE']

files = list_s3_files(bucket_name, S3_PATH_PREFIX, keywords)
error_df = process_file_errors(bucket_name, files, cs)

try:
    write_pandas(ctx, df=error_df, table_name='PRICE_ERRORS')
except Exception as emsg:
    # This is error message is making the bold assumption that there actually ISN'T any data to load.
    # If we expect data to load, but it's not in the relevant table, print(emsg) instead.
    print("No new data to load")