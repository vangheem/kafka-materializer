# Kafka Materializer

Mixing Cassandra with Kafka to be able to make materialized views from Kafka data streams.

Very simple POC :)


# Running this thing(local):

Install(required python poetry):

```
poetry install
```

Run dependencies:

```
docker-compose up
```

Bootstrap the data layers:

```
poetry run materializer bootstrap --env=.env.dev
```

Run API:

```
poetry run materializer api --env=.env.dev
```

Run Worker:

```
poetry run materializer worker --env=.env.dev
```


Generate Data:

```
poetry run materializer generate foobar --env=.env.dev
```

# API Usage

See all APIs to going to `http://localhost:5000/redoc` after startup.


Create view:

```
 'localhost:5000/views' \
--header 'Content-Type: application/json' \
--data-raw '{"topic": "foobar", "order_by": "name", "fields": ["address"]}'
```

Get views:

```
curl --location --request GET 'localhost:5000/views'
```

Query view:

```
curl --location --request GET 'localhost:5000/views/<view id>/query'
```