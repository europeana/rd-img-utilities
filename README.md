# Downloading images from S3

to do: permissions csv file
to do: find location postgres database
to do: add parallelization script

## Start container

```shell
docker-compose up -d
```

```shell
docker-compose exec development_container bash
```

## Build and populate the database

nohup python3 scripts/build_db.py &> /storage/logs/build_db.out &

## Download images

the input should be a csv file with a column 'europeana_id'


nohup python3 scripts/download_images.py --input /storage/data/sample.csv --output /storage/data/thumbnails &> /storage/logs/download_images.out &



## Database operations



docker-compose exec db bash

Create database

docker-compose exec db createdb -U myuser mydatabase

Remove database

docker-compose exec db dropdb -U myuser mydatabase





















python3 scripts/query_database.py

nohup python3 scripts/api.py &> /storage/logs/api.out &

python3 scripts/query_database_api.py
