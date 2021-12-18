from pydantic import BaseSettings
from typing import List


class Settings(BaseSettings):
    api_port: int = 5000

    kafka_servers: List[str]

    cassandra_servers: List[str]
    cassandra_keyspace: str = "materializer"
    cassandra_replication: str = "{'class':'SimpleStrategy', 'replication_factor' : 3};"
