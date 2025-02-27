
import os

from simplespark.environment.config import SimpleSparkConfig, WorkerConfig
from simplespark.utils.network import get_host_ip


class SimpleSparkEnvironment:

    def __init__(self, config: SimpleSparkConfig):

        self.config = config
        self.simple_home = os.environ.get("SIMPLE_SPARK_HOME", None)

        # FIXME replace with getter funciton
        self.libs_path = f"{self.simple_home}/libs"

        self.package_urls: dict[str, str] = {
            "java": "https://github.com/adoptium/temurin11-binaries/releases/download"
                    f"/jdk-{config.get_package_version('java').replace('+', '%2B')}",
            "scala": f"https://downloads.lightbend.com/scala/{config.get_package_version('scala')}",
            "spark": f"https://downloads.apache.org/spark/spark-{config.get_package_version('spark')}",
            # "spark": f"https://archive.apache.org/dist/spark/spark-{config.get_package_version('spark')}",
        }

        self.package_names: dict[str, str] = {
            "java": f"OpenJDK11U-jdk_x64_linux_hotspot_{config.get_package_version('java').replace('+', '_')}",
            "scala": f"scala-{config.get_package_version('scala')}",
            "spark": f"spark-{config.get_package_version('spark')}-bin-hadoop3"
        }

        self.package_extensions: dict[str, str] = {
            "java": "tar.gz",
            "scala": "tgz",
            "spark": "tgz"
        }

    def activate_environment(self):
        os.system(f"source {self.get_activate_script_path()}")

    def archive_name(self, package: str) -> str:
        return f"{self.package_names[package]}.{self.package_extensions[package]}"

    def full_package_url(self, package: str) -> str:
        return f"{self.package_urls[package]}/{self.archive_name(package)}"

    def libs_directory(self) -> str:
        return f"{self.simple_home}/libs"

    def get_package_libs_directory(self, package: str) -> str:
        return f"{self.libs_directory()}/{package}"

    def get_package_home_directory(self, package: str) -> str:
        return f"{self.get_package_libs_directory(package)}/{self.config.get_package_version(package)}"

    def spark_home(self) -> str:
        return self.get_package_home_directory("spark")

    def spark_jars_path(self) -> str:
        return f"{self.spark_home()}/jars"

    def spark_config_path(self) -> str:
        return f"{self.spark_home()}/conf/spark-defaults.conf"

    def spark_env_sh_path(self) -> str:
        return f"{self.spark_home()}/conf/spark-env.sh"

    def get_activate_script_directory(self) -> str:
        return f"{self.simple_home}/activate"

    def get_activate_script_path(self) -> str:
        return f"{self.get_activate_script_directory()}/{self.config.name}.sh"

    def hive_config_path(self) -> str:
        return f"{self.spark_home()}/conf/hive-site.xml"

    def is_driver(self, host: str) -> bool:
        return self.config.driver.host == host

    def get_worker_config(self, host: str) -> WorkerConfig | None:
        worker_config = None
        for worker in self.config.workers:
            if host == worker.host:
                worker_config = worker
                break
        return worker_config