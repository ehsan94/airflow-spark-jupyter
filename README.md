# Steup
```sh
docker build . -f Dockerfile --pull --tag my-image:0.0.7
```

Change the airflow image version in compose.

```sh
docker-compose up airflow-init
```

```sh
docker-compose up
```
### In notebook container
```sh
pip install tweepy==3.10.0
pip install pyspark==3.1.2
```