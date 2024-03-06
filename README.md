# Europeana R&D image utilities

This repository contains the tools for accessing and dowloading images from the IBM S3 bucket. 

A Postgres database is created for the mapping of object ids and image hashes


## Start containers

```shell
docker-compose up -d
```

## Build and populate the database

```shell
docker-compose exec development_container bash
```

```shell
nohup python3 scripts/build_db.py &> /storage/logs/build_db.out &
```
## Download images

add IBM cloud credentials as environment variables

```shell
export COS_API_KEY_ID="your_COS_API_KEY_ID_here"
export COS_INSTANCE_CRN="your_COS_INSTANCE_CRN_here"
export COS_ENDPOINT="your_COS_ENDPOINT_here"
```

the input should be a csv file with a column 'europeana_id'

```shell
nohup python3 scripts/download_images.py --input /storage/data/sample.csv --output /storage/data/thumbnails2 &> /storage/logs/download_images.out &
```


## Database operations

```shell
docker-compose exec db bash
```

Create database

```shell
docker-compose exec db createdb -U myuser mydatabase
```
Remove database

```shell
docker-compose exec db dropdb -U myuser mydatabase
```




















python3 scripts/query_database.py

nohup python3 scripts/api.py &> /storage/logs/api.out &

python3 scripts/query_database_api.py
