import fire
import time
import os
from multiprocessing import Pool
import pandas as pd
import psycopg2
from pathlib import Path
import ibm_boto3
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


def search_objects(row):
    prefix = row['ThumbnailID']
    europeana_id = row['ProvidedCHO'].replace('/','[ph]')
    object_summary_iterator = bucket.objects.filter(Prefix = prefix)
    for item in object_summary_iterator:
        try:
            object = client.Object("europeana-thumbnails-production", item.key)
            if object.key.endswith('MEDIUM'):
                object.download_file(output_path.joinpath(f'{europeana_id}.jpg'))
        except:
            pass


if __name__ == "__main__":

    COS_API_KEY_ID = os.environ['COS_API_KEY_ID']
    COS_INSTANCE_CRN = os.environ['COS_INSTANCE_CRN']
    COS_ENDPOINT = os.environ['COS_ENDPOINT']

    processes = 6

    parser = argparse.ArgumentParser(description="Just an example",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)

    args = parser.parse_args()
    input_path = args.input
    output_path = args.output

    output_path = Path(output_path)
    output_path.mkdir(exist_ok = True, parents = True)

    input_df = pd.read_csv(input_path)
    input_CHOs = input_df['europeana_id'].values

    db = HashDatabase()
    #print(db.size())

    print('Searching hashes...')
    objects = db.search(input_CHOs)
    print('Finished finding hashes')
    output_df = pd.DataFrame.from_dict(objects)
    print(f'Number of hashes found: {output_df.shape[0]}')

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
      result = list(tqdm(pool.imap(search_objects, rows)))
    finish_time = time.perf_counter()
    print("Finished, it took {} minutes".format((finish_time-start_time)/60.0))






    








