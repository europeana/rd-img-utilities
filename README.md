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

Add IBM and AWS cloud credentials as environment variables

```shell
export IBM_API_KEY_ID="your_COS_API_KEY_ID_here"
export IBM_INSTANCE_CRN="your_COS_INSTANCE_CRN_here"
export IBM_ENDPOINT="your_COS_ENDPOINT_here"
export AWS_ACCESS_KEY_ID="your_AWS_ACCESS_KEY_ID_here"
export AWS_SECRET_ACCESS_KEY="your_AWS_SECRET_ACCESS_KEY_here"
```


the input should be a csv file with a column 'europeana_id'

```shell
nohup python3 scripts/download_images.py --suffix 'LARGE' --input /storage/data/small_sample.csv --output /storage/data/thumbnails_LARGE &> /storage/logs/download_images.out &
```

Jupyter notebook 

```shell
jupyter notebook --port 5053 --ip 0.0.0.0 --no-browser --allow-root
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

