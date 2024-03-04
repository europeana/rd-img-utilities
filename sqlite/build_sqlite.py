import pandas as pd
import fire
import time
from tqdm import tqdm
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def main():

    csv_file_path = '/storage/cho-thumbnail-map.csv'

    # Use a chunksize if the file is too large to fit in memory
    chunksize = 10 ** 5  # Adjust based on your available memory
    #chunksize = 10 ** 3  # Adjust based on your available memory

    Base = declarative_base()

    class MyCSVData(Base):
        __tablename__ = 'hash_mapping'
        id = Column(Integer, primary_key=True)
        ProvidedCHO = Column(String)
        ThumbnailURL = Column(String)
        ThumbnailID = Column(String)

    #database_url = 'sqlite:////storage/hash_database.db'
    database_url = 'mysql+mysqlconnector:////storage/hash_database.db'

    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def import_chunk_to_db(chunk, engine):
        chunk.to_sql('hash_mapping', con=engine, if_exists='append', index=False)

    # Iterate over the CSV in chunks

    print('Populating database...')

    start = time.time()

    for chunk in tqdm(pd.read_csv(csv_file_path, chunksize=chunksize, nrows = 10000)):
        chunk.columns = chunk.columns.str.replace(' ','')
        chunk['ProvidedCHO'] = chunk['ProvidedCHO'].apply(lambda x: x.replace('http://data.europeana.eu/item',''))
        import_chunk_to_db(chunk, engine)

    end = time.time()

    print(f'Finished, it took {(end-start)/60.0} minutes')

    #with engine.connect() as conn:
    #    conn.execute("CREATE INDEX idx_ProvidedCHO ON hash_mapping ('ProvidedCHO');")
    #    conn.execute("CREATE INDEX idx_ThumbnailURL ON hash_mapping ('ThumbnailURL');")
    #    conn.execute("CREATE INDEX idx_ThumbnailID ON hash_mapping ('ThumbnailID');")

if __name__ == '__main__':
    fire.Fire(main)



