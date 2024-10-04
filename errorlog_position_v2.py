# File handling here is slightly janky since the repo has 28k files, but seems performant thus far.
# Ideally would split the errorlogs into multiple folders so we don't have to iterate all.

import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import io
import json
import pandas as pd
import re
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
    last_week = datetime.now() - timedelta(days=5)

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

def filter_file_extension(file_list, extension):
    '''
    params:
        file_list: ...
        extension: the extension of the files we want to keep from the list
    returns:
        list with the extensions we've specified
    '''
    filtered_fl = [file for file in file_list if extension in file['Key']]

    return filtered_fl

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

        cs.execute(f"SELECT COUNT(*) FROM IM_REC.CR_EPHEMERAL.POSITION_ERRORS WHERE ELOG_FILE_NAME = %s", (key.replace('Extracts/IDH003/ErrorLogs/', ''),))
        file_exists = cs.fetchone()[0] > 0
        if file_exists:
            print(f"File {key} already loaded. Continue...")
            continue

        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            file_data = response['Body'].read().decode('utf-8')

            file_df = pd.read_csv(io.StringIO(file_data), names=['PORTFOLIO_ID', 'IDENTIFIER', 'STATUS',  'ERROR_MSG'])

            for _, row in file_df.iterrows():
                if row['STATUS'].strip().upper() == 'ERROR':
                    error_records.append({
                            'FILE_NAME': key.replace('Extracts/IDH003/ErrorLogs/', ''),
                            'PORTFOLIO_ID': row['PORTFOLIO_ID'],
                            'IDENTIFIER': row['IDENTIFIER'],
                            'STATUS': row['STATUS'],
                            'ERROR_MSG': row['ERROR_MSG'],
                            'LOAD_DATE_TIME': date
                        })
  
        except ClientError as emsg:
            print(f"Failed to retrieve {key} from S3: {emsg}")
        except Exception as emsg:
            print(f"Error processing file {key}: {emsg}")

    error_df = pd.DataFrame(error_records, columns=['FILE_NAME', 'PORTFOLIO_ID', 'IDENTIFIER', 'STATUS',  'ERROR_MSG', 'LOAD_DATE_TIME'])

    return error_df

def position_line_df(error_df):
    '''
    params:
        error_df: df containing any rows with ERROR from the error_logs
    returns:
        df consisting of the error_log name + error line_no etc..
    '''
    line_no_list = []
    line_no_re = re.compile(r'Line (\d+)')

    for idx, row in error_df.iterrows():
        error_msg = row['ERROR_MSG']
        pat_found = line_no_re.search(error_msg)

        if pat_found:
            line_no = pat_found.group(1)
            line_no_list.append({
                'FILE_NAME': row['FILE_NAME'],
                'PORTFOLIO_ID': row['PORTFOLIO_ID'],
                'IDENTIFIER': row['IDENTIFIER'],
                'STATUS': row['STATUS'],
                'ERROR_MSG': row['ERROR_MSG'],
                'LINE_NUMBER': line_no,
                'LOAD_DATE_TIME': row['LOAD_DATE_TIME']
            })
    position_loc_df = pd.DataFrame(line_no_list, columns=['FILE_NAME','PORTFOLIO_ID','IDENTIFIER','STATUS','ERROR_MSG', 'LINE_NUMBER','LOAD_DATE_TIME'])

    return position_loc_df

def locate_position_error(position_fl, position_loc_df):
    '''
    params:
        position_fl: the list of upload files sent to bloomberg
        position_loc_df: df consisting of the error_log name + error line_no etc..
    return:
        df with security identifiers from position_fl files concat with error_log
    '''
    errors = []

    for file in position_fl:
        key = file['Key']
        _search_key = file['Key'].replace('Extracts/IDH003/Archive/', '')

        relevant_rows = position_loc_df[position_loc_df['FILE_NAME'].str.contains(_search_key, regex=False)]

        if not relevant_rows.empty:
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=key)
                file_data = response['Body'].read().decode('utf-8')

                file_df = pd.read_csv(io.StringIO(file_data), names=['PORTFOLIO_ID', 'SECURITY_IDENTIFIER_SEDOL', 'SECURITY_IDENTIFIER_ISIN', 'SECURITY_IDENTIFIER_TICKER', 'SECURITY_IDENTIFIER_CUSIP', 'SECURITY_NAME', 'QUANTITY', 'WEIGHT', 'COST PRICE', 'MARKET PRICE', 'MARKET VALUE', 'DATE'])

                for _, row in relevant_rows.iterrows():
                    line_number = int(row['LINE_NUMBER'])-1

                    if line_number < len(file_df):
                        matching_row = file_df.iloc[line_number]
                        errors.append({
                            'UPLOAD_FILE_NAME': _search_key,
                            'ELOG_FILE_NAME': row['FILE_NAME'],
                            'PORTFOLIO_ID': matching_row['PORTFOLIO_ID'],
                            'SEDOL': matching_row['SECURITY_IDENTIFIER_SEDOL'],
                            'ISIN': matching_row['SECURITY_IDENTIFIER_ISIN'],
                            'TICKER': matching_row['SECURITY_IDENTIFIER_TICKER'],
                            'CUSIP': matching_row['SECURITY_IDENTIFIER_CUSIP'],
                            'SECURITY_NAME': matching_row['SECURITY_NAME'],
                            'STATUS': row['STATUS'],
                            'ERROR_MSG': row['ERROR_MSG'],
                            'LOAD_DATE_TIME': row['LOAD_DATE_TIME']
                            })

            except ClientError as emsg:
                print(f"Failed to retrieve {key} from S3: {emsg}")
            except Exception as emsg:
                print(f"Error processing file {key}: {emsg}")

    position_error_df = pd.DataFrame(errors)

    return position_error_df

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

keywords = ['IDH003_POSITION']

error_files = list_s3_files(bucket_name, S3_ERROR_PREFIX, keywords)
error_files = filter_file_extension(error_files, '.status.')
upload_files = list_s3_files(bucket_name, S3_UPLOAD_PREFIX, keywords)

error_df = process_file_errors(bucket_name, error_files, cs)
position_loc_df = position_line_df(error_df)
position_error_df = locate_position_error(upload_files, position_loc_df)

try:
    write_pandas(ctx, df=position_error_df, table_name='POSITION_ERRORS')
except Exception as emsg:
    # This is error message is making the bold assumption that there actually ISN'T any data to load.
    # If we expect data to load, but it's not in the relevant table, print(emsg) instead.
    print("No new data to load")