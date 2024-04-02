import fire
import time
import os
from multiprocessing import Pool
import pandas as pd
import psycopg2
from pathlib import Path
import ibm_boto3
import boto3
from ibm_botocore.client import Config
from functools import partial
import argparse
from tqdm import tqdm

class HashDatabase:
    def __init__(self):

        params = {
            'database': 'mydatabase',
            'user': 'myuser',
            'password': 'mypassword',
            'host': 'db',
            'port': 5432
        }

        conn = psycopg2.connect(**params)
        self.cur = conn.cursor()
    
    def size(self):
        self.cur.execute(f"SELECT COUNT(*) FROM hash_mapping")
        n_rows = self.cur.fetchone()[0]
        return n_rows

    def search(self,input_CHOs):
        placeholders = ', '.join(['%s'] * len(input_CHOs))
        self.cur.execute(f'SELECT * FROM hash_mapping WHERE "ProvidedCHO" IN ({placeholders})',input_CHOs)
        rows = self.cur.fetchall()

        objects = []
        for row in rows:
            objects.append({"ProvidedCHO": row[1], "ThumbnailURL": row[2], "ThumbnailID": row[3]})

        return objects


def download_objects_IBM_function(row):
    suffix = 'MEDIUM'
    prefix = row['ThumbnailID']
    europeana_id = row['ProvidedCHO'].replace('/','[ph]')
    object_summary_iterator = bucket.objects.filter(Prefix = prefix)
    found_filenames = []
    for item in object_summary_iterator:
        try:
            object = client.Object("europeana-thumbnails-production", item.key)
            if object.key.endswith(suffix):
                object.download_file(output_path.joinpath(f'{item.key}.jpg'))
                found_filenames.append(item.key)
            
        except:
            pass

def download_objects_AWS_function(row):
    bucket_name = 'europeana-thumbnails-production'
    suffix = 'MEDIUM'
    prefix = row['ThumbnailID']
    europeana_id = row['ProvidedCHO'].replace('/','[ph]')

    object_summary_iterator = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    found_filenames = []
    for item in object_summary_iterator.get('Contents', []):
        try:
            file_name = item['Key']
            if file_name.endswith(suffix):
                download_path = output_path.joinpath(f'{item.key}.jpg')
                s3.download_file(bucket_name, file_name, download_path)
                found_filenames.append(file_name)
        except:
            pass


if __name__ == "__main__":

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    args = parser.parse_args()

    suffix = 'MEDIUM'

    input_path = args.input
    input_df = pd.read_csv(input_path)
    input_CHOs = input_df['europeana_id'].values

    output_path = args.output
    output_path = Path(output_path)
    output_path.mkdir(exist_ok = True, parents = True)


    print('Searching hashes in database...')
    db = HashDatabase()
    objects = db.search(input_CHOs)
    print('Finished finding hashes')
    output_df = pd.DataFrame.from_dict(objects)
    print(f'Number of hashes found: {output_df.shape[0]}')


    processes = 6


    # Search IBM 
    print('IBM')

    COS_API_KEY_ID = os.environ['IBM_API_KEY_ID']
    COS_INSTANCE_CRN = os.environ['IBM_INSTANCE_CRN']
    COS_ENDPOINT = os.environ['IBM_ENDPOINT']

    client = ibm_boto3.resource("s3",
        ibm_api_key_id=COS_API_KEY_ID,
        ibm_service_instance_id=COS_INSTANCE_CRN,
        config=Config(signature_version="oauth"),
        endpoint_url=COS_ENDPOINT
    )

    bucket = client.Bucket('europeana-thumbnails-production')
    rows = [row for index, row in output_df.iterrows()]
    print('Searching and downloading images...')
    start_time = time.perf_counter()
    with Pool(processes=processes) as pool:
      result = list(tqdm(pool.imap(download_objects_IBM_function, rows)))
    finish_time = time.perf_counter()
    ibm_time = (finish_time-start_time)/60.0
    print("Finished, it took {} minutes".format(ibm_time))


    # Filter output_df with found_filenames
    found_filenames = [file.stem.replace(f'-{suffix}','') for file in output_path.iterdir() if file.is_file()]
    print(f'Images downloaded from IBM: {len(found_filenames)}')
    output_df = output_df.loc[output_df['ThumbnailID'].apply(lambda x: x not in found_filenames)]
    print(f'Images to search in AWS: {output_df.shape[0]}')
    

    # Search AWS

    print('AWS')

    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

    s3 = boto3.client('s3', 
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)

    rows = [row for index, row in output_df.iterrows()]
    print('Searching and downloading images...')
    start_time = time.perf_counter()
    with Pool(processes=processes) as pool:
      result = list(tqdm(pool.imap(download_objects_AWS_function, rows)))
    finish_time = time.perf_counter()
    aws_time = (finish_time-start_time)/60.0
    print("Finished, it took {} minutes".format(aws_time))

    print("Total time: {} minutes".format(ibm_time+aws_time))













    








