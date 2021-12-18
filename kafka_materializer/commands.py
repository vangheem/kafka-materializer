from typing import Optional
import faker
import orjson
from kafka import KafkaProducer
import typer

from kafka_materializer.api import API
from kafka_materializer.worker import Worker
from .settings import Settings
from .data.materializer import MaterializerDataService
import dotenv
import uvicorn

app = typer.Typer()


def _get_settings(env_file: Optional[str] = None) -> Settings:
    if env_file is not None:
        dotenv.load_dotenv(env_file)
    return Settings()


@app.command()
def api(env: Optional[str] = None):
    settings = _get_settings(env)
    uvicorn.run(API(settings), port=settings.api_port)


@app.command()
def worker(env: Optional[str] = None):
    settings = _get_settings(env)

    worker = Worker(settings)
    worker()


@app.command()
def bootstrap(env: Optional[str] = None):
    settings = _get_settings(env)

    mds = MaterializerDataService(settings)
    mds.bootstrap()


@app.command()
def generate(topic: str, env: Optional[str] = None):
    settings = _get_settings(env)

    producer = KafkaProducer(bootstrap_servers=settings.kafka_servers)

    fake = faker.Faker()
    count = 0
    while True:
        producer.send(
            topic,
            orjson.dumps(
                {
                    "name": fake.name(),
                    "address": fake.address(),
                    "ip": fake.ipv4_private(),
                    "description": fake.sentence(),
                }
            ),
        )
        count += 1
        if count % 100 == 0:
            print(f"Produced {count}")


if __name__ == "__main__":
    app()
