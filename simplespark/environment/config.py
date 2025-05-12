import os
from dataclasses import dataclass, asdict
import json
from typing import Any, Dict, List


@dataclass
class DriverConfig:
    host: str
    cores: int = None
    memory: str = None
    connect_server: bool = False
    # history_server: bool = False
    thrift_server: bool = False


@dataclass
class PackageConfig:
    name: str
    version: str

    @property
    def package_file_name(self) -> str:

        NAME_MAP = {
            "java": f"OpenJDK8U-jdk_x64_linux_hotspot_"
                    f"{self.version.replace('+', '_').replace('-', '')}.tar.gz",
            "scala": f"scala-{self.version}.tgz",
            "spark": f"spark-{self.version}-bin-hadoop3.tgz"
        }

        package_file_name = NAME_MAP.get(self.name)
        assert package_file_name is not None

        return package_file_name

    @property
    def package_releases_url(self) -> str:

        URL_MAP: dict[str, str] = {
            "java": "https://github.com/adoptium/temurin8-binaries/releases/download",
            "scala": "https://downloads.lightbend.com/scala",
            "spark": "https://downloads.apache.org/spark",
            # "spark": f"https://archive.apache.org/dist/spark/",
        }

        package_releases_url = URL_MAP.get(self.name)
        assert package_releases_url is not None

        return package_releases_url


    @property
    def package_version_directory(self) -> str:

        DIRECTORY_MAP: dict[str, str] = {
            "java": f"jdk{self.version.replace('+', '%2B')}",
            "scala": self.version,
            "spark": f"spark-{self.version}",
        }

        package_version_directory = DIRECTORY_MAP.get(self.name)
        assert package_version_directory is not None

        return package_version_directory

    @property
    def package_download_url(self) -> str:
        return f'{self.package_releases_url}/{self.package_version_directory}/{self.package_file_name}'


@dataclass
class WorkerConfig:
    host: str
    cores: int = None
    memory: str = None
    instances: int = None


@dataclass
class MavenConfig:
    group_id: str
    artifact_id: str
    version: str
    driver: str = ""
    jdbc_prefix: str = ""


@dataclass
class JdbcConfig:
    config_name: str
    db_connector: MavenConfig
    db_host: str
    db_port: int
    db_user: str
    db_pass: str

    def get_url(self, database = '') -> str:
        return f"jdbc:{self.db_connector.jdbc_prefix}://{self.db_host}:{self.db_port}/{database}"


@dataclass
class SimpleSparkConfig:
    name: str
    simplespark_home: str
    bash_profile_file: str
    mode: str
    packages: List[PackageConfig]
    driver: DriverConfig
    derby_path: str = None
    warehouse_path: str = None
    metastore_config: JdbcConfig = None
    workers: List[WorkerConfig] = None
    executor_memory: str = None
    jdbc_drivers: Dict[str, MavenConfig] = None

    def __post_init__(self):
        self._package_map: dict[str, PackageConfig] = {p.name: p for p in self.packages}

    def __str__(self) -> str:

        config_json = self.to_json()
        json_string = json.dumps(config_json, indent=2)

        return json_string

    @staticmethod
    def get_simplespark_config(environment_name: str):

        simplespark_home = os.environ.get("SIMPLESPARK_HOME", None)
        if simplespark_home is None:
            raise Exception("SIMPLESPARK_HOME environment variable not set, need to run `build` first")

        config = SimpleSparkConfig.read(f"{simplespark_home}/environments/{environment_name}/{environment_name}.json")

        return config

    @staticmethod
    def get_field_deserializers() -> Dict[str, callable]:

        # Write deserializers as lambdas to not require all fields to be defined
        deserializers = {
            'driver': lambda c: DriverConfig(**c['driver']),
            'packages': lambda c: [PackageConfig(**p) for p in c['packages']],
            'workers': lambda c: [WorkerConfig(**w) for w in c['workers']],
            'metastore_config': lambda c: JdbcConfig(**c['metastore_config']),
            'jdbc_drivers': lambda c: {k: MavenConfig(**v) for k, v in c['jdbc_drivers'].items()}
        }

        return deserializers

    @staticmethod
    def from_json(json_dict):

        for field_name, deserializer in SimpleSparkConfig.get_field_deserializers().items():
            if field_name in json_dict:
                json_dict[field_name] = deserializer(json_dict)

        return SimpleSparkConfig(**json_dict)

    @staticmethod
    def read(*json_path: str):

        config_dict = {}

        for path in json_path:
            print(f'Reading {path}')
            with open(path, 'r') as read_file:
                config_dict = config_dict | json.load(read_file)

        return SimpleSparkConfig.from_json(config_dict)

    @property
    def activate_script_directory(self) -> str:
        return f"{self.simplespark_home}/activate"

    @property
    def activate_script_path(self) -> str:
        return f"{self.activate_script_directory}/{self.name}.env"

    @property
    def hive_config_path(self) -> str:
        return f"{self.spark_conf_directory}/hive-site.xml"

    @property
    def simplespark_bin_directory(self) -> str:
        return f"{self.simplespark_home}/bin"

    @property
    def simplespark_config_file_path(self) -> str:
        return f"{self.simplespark_environment_directory}/{self.name}/config.json"

    @property
    def simplespark_environment_directory(self) -> str:
        return f"{self.simplespark_home}/environments"

    @property
    def simplespark_libs_directory(self) -> str:
        return f"{self.simplespark_home}/libs"

    @property
    def spark_home(self) -> str:
        return self.get_package_home_directory('spark')

    @property
    def spark_conf_directory(self) -> str:
        return f"{self.simplespark_environment_directory}/{self.name}/conf"

    @property
    def spark_conf_file_path(self) -> str:
        return f"{self.spark_conf_directory}/spark-defaults.conf"

    @property
    def spark_env_sh_path(self) -> str:
        return f"{self.spark_conf_directory}/spark-env.sh"

    @property
    def spark_jars_path(self) -> str:
        return f"{self.spark_home}/jars"

    @property
    def spark_master(self) -> str:
        return f"spark://{self.driver.host}:7077"

    def get_package_config(self, package: str) -> PackageConfig:
        if not self.has_package(package):
            raise Exception(f"Package {package} does not defined in config")
        return self._package_map[package]

    def get_package_home_directory(self, package: str) -> str:
        return f"{self.simplespark_libs_directory}/{package}/{self.get_package_version(package)}"

    def get_package_version(self, package_name: str) -> str:
        package_config = self.get_package_config(package_name)
        return package_config.version

    def get_worker_config(self, host: str) -> WorkerConfig | None:
        worker_config = None
        for worker in self.workers:
            if host == worker.host:
                worker_config = worker
                break
        return worker_config

    def has_package(self, package: str) -> bool:
        return package in self._package_map

    def to_json(self, remove_nulls: bool = True) -> str:

        def remove_nulls_from_dict(d: dict[str, Any]):
            for k, v in d.copy().items():
                if v is None:
                    del d[k]
                elif isinstance(v, dict):
                    remove_nulls_from_dict(v)

        config_json = asdict(self)
        if remove_nulls:
            remove_nulls_from_dict(config_json)

        return config_json

    def write(self, json_path: str = ''):

        if json_path == '':
            json_path = self.simplespark_config_file_path

        as_string = str(self)

        with open(json_path, 'w') as write_file:
            write_file.write(as_string)
