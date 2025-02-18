
from dataclasses import dataclass, asdict
import json
from typing import Any, Dict, List


@dataclass
class ResourceConfig:
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
class MavenJar:
    group_id: str
    artifact_id: str
    version: str


@dataclass
class SimpleSparkConfig:
    name: str
    simple_home: str
    profile_path: str
    packages: Dict[str, str]
    driver: ResourceConfig
    derby_path: str = None
    warehouse_path: str = None
    metastore_config: JdbcConfig = None
    workers: List[ResourceConfig] = None
    executor_memory: str = None
    jdbc_drivers: dict[str, MavenJar] = None

    def __str__(self) -> str:

        config_dict = asdict(self)
        json_string = json.dumps(config_dict, indent=2)

        return json_string

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
            'driver': lambda c: ResourceConfig(**c['driver']),
            'workers': lambda c: list(map(lambda x: ResourceConfig(**x), c['workers'])),
            'metastore_config': lambda c: JdbcConfig(**c['metastore_config']),
            'jbc_drivers': lambda c: {k: MavenJar(**v) for k, v in c['jdbc_drivers'].items()}
        }

        return deserializers

    @staticmethod
    def read(*json_path: str):

        config_dict = {}

        for path in json_path:
            with open(path, 'r') as read_file:
                config_dict = config_dict | json.load(read_file)

        for field_name, deserializer in SimpleSparkConfig.get_field_deserializers().items():
            if field_name in config_dict:
                config_dict[field_name] = deserializer(config_dict)
            config_dict[field_name] = deserializer(config_dict)

        return SimpleSparkConfig(**config_dict)

    @staticmethod
    def generate_template_json() -> str:

        template = SimpleSparkConfig(
            name='<SETUP-NAME>',
            simple_home='<SIMPLE-SPARK-DIR>',
            profile_path='<PATH-TO-PROFILE-FILE>',
            packages={
                'java': '11.0.21+9',
                'scala': '2.12.18',
                'spark': '3.5.2',
                'delta': '3.2.0',
                'hadoop': '3.3.1',
                'hive': '3.1.2'
            },
            driver=ResourceConfig(
                host='<DRIVER-IP-ADDRESS>',
                cores=4,
                memory='8G'
            ),
            derby_path='<OPTIONAL-PATH-TO-LOCAL-DERBY-CATALOG>',
            warehouse_path='<OPTIONAL-PATH-TO-LOCAL-WAREHOUSE>',
            metastore_config=JdbcConfig(
                db_type='<mysql/postgres/oracle>',
                db_host='<DATABASE-IP-ADDRESS>',
                db_port=0,
                db_user='<DATABASE-LOGIN-USERNAME>',
                db_pass='<DATABASE-LOGIN-PASSWORD>',
                jdbc_driver='<JAVA-PATH-TO-CLASS>'
            ),
            workers=[
                ResourceConfig(
                    host='<WORKER-IP-ADDRESS>',
                    cores=4,
                    memory='8G',
                    instances=8
                )
            ],
            executor_memory='8G',
            jdbc_drivers={
                'postgres': MavenJar("org.postgresql", "postgresql", "42.7.4")
            }
        )

        return str(template)
