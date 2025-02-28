
from dataclasses import dataclass, asdict
import json
from typing import Any, Dict, List


@dataclass
class DriverConfig:
    host: str
    cores: int = None
    memory: str = None


@dataclass
class WorkerConfig:
    host: str
    cores: int = None
    memory: str = None
    instances: int = None


@dataclass
class JdbcConfig:
    db_type: str
    db_host: str
    db_port: int
    db_user: str
    db_pass: str
    jdbc_driver: str

    def get_url(self) -> str:

        URL_PREFIXES = {
            "mssql": "sqlserver",
            "mysql": "mysql",
            "oracle": "oracle",
            "postgres": "postgresql"
        }
        prefix = URL_PREFIXES[self.db_type]

        return f"jdbc:{prefix}://{self.db_host}:{self.db_port}/metastore_db"


@dataclass
class MavenConfig:
    group_id: str
    artifact_id: str
    version: str


@dataclass
class SimpleSparkConfig:
    name: str
    setup_type: str
    packages: Dict[str, str]
    driver: DriverConfig
    derby_path: str = None
    warehouse_path: str = None
    metastore_config: JdbcConfig = None
    workers: List[WorkerConfig] = None
    executor_memory: str = None
    jdbc_drivers: Dict[str, MavenConfig] = None

    def __str__(self) -> str:

        config_json = self.get_as_json()
        json_string = json.dumps(config_json, indent=2)

        return json_string

    def get_as_json(self):

        def remove_nulls(d: dict[str, Any]):
            for k, v in d.copy().items():
                if v is None:
                    del d[k]
                elif isinstance(v, dict):
                    remove_nulls(v)

        config_json = asdict(self)
        remove_nulls(config_json)

        return config_json

    def get_package_version(self, package_name: str) -> str:
        return self.packages[package_name]

    def write(self, json_path: str):

        as_string = str(self)

        with open(json_path, 'w') as write_file:
            write_file.write(as_string)

    @staticmethod
    def get_field_deserializers() -> Dict[str, callable]:

        # Write deserializers as lambdas to not require all fields to be defined
        deserializers = {
            'driver': lambda c: DriverConfig(**c['driver']),
            'workers': lambda c: list(map(lambda x: WorkerConfig(**x), c['workers'])),
            'metastore_config': lambda c: JdbcConfig(**c['metastore_config']),
            'jbc_drivers': lambda c: {k: MavenConfig(**v) for k, v in c['jdbc_drivers'].items()}
        }

        return deserializers

    @staticmethod
    def read(*json_path: str):

        config_dict = {}

        for path in json_path:
            print(f'Reading {path}')
            with open(path, 'r') as read_file:
                config_dict = config_dict | json.load(read_file)

        for field_name, deserializer in SimpleSparkConfig.get_field_deserializers().items():
            if field_name in config_dict:
                config_dict[field_name] = deserializer(config_dict)

        return SimpleSparkConfig(**config_dict)
