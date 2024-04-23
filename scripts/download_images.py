import fire
import time
import os
from multiprocessing import Pool
from functools import partial
import pandas as pd
import psycopg2
from pathlib import Path
import ibm_boto3
import boto3
from ibm_botocore.client import Config
import argparse
from tqdm import tqdm
import concurrent.futures

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
        
        rows = []
        batch_size = 50000

        for i in range(0, len(input_CHOs), batch_size):
            batch = input_CHOs[i:i+batch_size]
            placeholders = ', '.join(['%s'] * len(batch))
            query = f'SELECT * FROM hash_mapping WHERE "ProvidedCHO" IN ({placeholders})'
            self.cur.execute(query, batch)
            rows += self.cur.fetchall()

        objects = []
        for row in rows:
            objects.append({"ProvidedCHO": row[1], "ThumbnailURL": row[2], "ThumbnailID": row[3]})

        return objects


def download_objects_IBM_function(row, output_path, suffix='MEDIUM'):
    if 'category' in row:
        category_path = output_path.joinpath(row['category'])
    else:
        category_path = output_path

    category_path.mkdir(parents=True, exist_ok=True)

    prefix = row['ThumbnailID']
    europeana_id = row['ProvidedCHO'].replace('/', '[ph]')
    fname = europeana_id + '.jpg'
    download_path = category_path.joinpath(fname)

    object_summary_iterator = bucket.objects.filter(Prefix=prefix)
    for item in object_summary_iterator:
        try:
            object = client.Object("europeana-thumbnails-production", item.key)
            if object.key.endswith(suffix):
                object.download_file(str(download_path))  # Ensure the path is string if necessary
        except Exception as e:
            print(f"Failed to download {item.key}: {e}")

def download_objects_AWS_function(row, output_path, suffix='MEDIUM'):
    if 'category' in row:
        category_path = output_path.joinpath(row['category'])
    else:
        category_path = output_path

    category_path.mkdir(parents=True, exist_ok=True)

    prefix = row['ThumbnailID']
    europeana_id = row['ProvidedCHO'].replace('/', '[ph]')
    fname = europeana_id + '.jpg'
    download_path = category_path.joinpath(fname)

    object_summary_iterator = s3.list_objects_v2(Bucket='europeana-thumbnails-production', Prefix=prefix)
    for item in object_summary_iterator.get('Contents', []):
        try:
            file_name = item['Key']
            if file_name.endswith(suffix):
                s3.download_file('europeana-thumbnails-production', file_name, str(download_path))
        except Exception as e:
            print(f"Failed to download {file_name}: {e}")


def process_file(entry):
    if entry.is_file():
        return entry.name.split('.')[0].replace('[ph]', '/')
    return None

def get_downloaded_ids(output_path):
    downloaded_ids = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_file, entry) for entry in os.scandir(output_path)}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                downloaded_ids.append(result)
    return downloaded_ids



if __name__ == "__main__":

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--suffix", type=str, nargs = '?', const='MEDIUM')
    parser.add_argument("--processes", type=int, nargs = '?', const=6)
    parser.add_argument("--sample", type=float, nargs = '?', const=1.0)
    parser.add_argument("--labeled", type=bool, nargs = '?', const=False)
    args = parser.parse_args()

    processes = args.processes
    suffix = args.suffix
    
    input_path = args.input
    input_df = pd.read_csv(input_path)
    if args.sample != 1.0:
        input_df = input_df.sample(frac = args.sample)

    input_CHOs = input_df['europeana_id'].values


    output_path = args.output
    output_path = Path(output_path)
    output_path.mkdir(exist_ok = True, parents = True)

    print('Searching hashes in database...')
    db = HashDatabase()
    objects = db.search(input_CHOs)
    print('Finished finding hashes')
    hash_df = pd.DataFrame.from_dict(objects)
    print(f'Number of hashes found: {hash_df.shape[0]}')

    #dowloaded_ids = get_downloaded_ids(output_path)
    #hash_df = hash_df.loc[hash_df['ProvidedCHO'].apply(lambda x: x not in dowloaded_ids)]

    provided_cho_set = set(hash_df['ProvidedCHO'])
    input_df = input_df[input_df['europeana_id'].isin(provided_cho_set)]
    if 'category' in input_df.columns:
        hash_df = hash_df.merge(input_df[['europeana_id', 'category']], left_on='ProvidedCHO', right_on='europeana_id', how='left')

    print(f'Number of images to download: {hash_df.shape[0]}')

    # Search IBM 
    print('Searching and downloading images in IBM...')

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
    rows = hash_df.to_dict('records')
    
    start_time = time.perf_counter()
    with Pool(processes=args.processes) as pool:
        partial_func = partial(download_objects_IBM_function, output_path=output_path, suffix=args.suffix)
        list(tqdm(pool.imap(partial_func, rows), total=len(rows), desc="Downloading Images from IBM"))


    finish_time = time.perf_counter()
    ibm_time = (finish_time-start_time)/60.0
    print("Finished, it took {} minutes".format(ibm_time))


    # Filter hash_df with already downloaded images
    dowloaded_ids = get_downloaded_ids(output_path)
    hash_df = hash_df.loc[hash_df['ProvidedCHO'].apply(lambda x: x not in dowloaded_ids)]
    print(f'Images downloaded from IBM: {len(dowloaded_ids)}')
    print(f'Images to search in AWS: {hash_df.shape[0]}')
    

    # Search AWS
    print('Searching and downloading images in AWS...')

    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

    s3 = boto3.client('s3', 
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)

    rows = hash_df.to_dict('records')
    
    start_time = time.perf_counter()
    with Pool(processes=args.processes) as pool:
        partial_func = partial(download_objects_AWS_function, output_path=output_path, suffix=args.suffix)
        list(tqdm(pool.imap(partial_func, rows), total=len(rows), desc="Downloading Images from IBM"))

    finish_time = time.perf_counter()
    aws_time = (finish_time-start_time)/60.0
    print("Finished, it took {} minutes".format(aws_time))

    print("Total time: {} minutes".format(ibm_time+aws_time))













    








