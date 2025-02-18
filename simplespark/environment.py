
from simplespark.config import SimpleSparkConfig, ResourceConfig
from simplespark.utils.network import get_host_ip


class SimpleSparkEnvironment:

    def __init__(self, config: SimpleSparkConfig, local_host: str = ''):

        self.config = config
        self.libs_path = f"{config.simple_home}/libs"

        if local_host == '':
            print('Local host not set, detecting automatically')
            local_host = get_host_ip()
            print(f'Local IP detected: {local_host}')
        self.local_host = local_host

        self.is_driver = local_host == self.config.driver.host

        def _get_resource_config() -> ResourceConfig:

            if self.is_driver:
                return self.config.driver

            for worker in self.config.workers:
                is_worker = local_host == worker.host
                if is_worker:
                    return worker

            raise Exception(f'Not able to identify resource with host: {local_host}')

        self.resource_config = _get_resource_config()

        self.package_urls: dict[str, str] = {
            "java": "https://github.com/adoptium/temurin11-binaries/releases/download"
                    f"/jdk-{config.get_package_version('java').replace('+', '%2B')}",
            "scala": f"https://downloads.lightbend.com/scala/{config.get_package_version('scala')}",
            "spark": f"https://archive.apache.org/dist/spark/spark-{config.get_package_version('spark')}",
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

    def archive_name(self, package: str) -> str:
        return f"{self.package_names[package]}.{self.package_extensions[package]}"

    def full_package_url(self, package: str) -> str:
        return f"{self.package_urls[package]}/{self.archive_name(package)}"

    def libs_directory(self) -> str:
        return f"{self.config.simple_home}/libs"

    def package_home_directory(self, package: str) -> str:
        return f"{self.libs_directory()}/{self.package_names[package]}"

    def spark_home(self) -> str:
        return self.package_home_directory("spark")

    def spark_jars_path(self) -> str:
        return f"{self.spark_home()}/jars"

    def spark_config_path(self) -> str:
        return f"{self.spark_home()}/conf/spark-defaults.conf"

    def spark_env_sh_path(self) -> str:
        return f"{self.spark_home()}/conf/spark-env.sh"

    def hive_config_path(self) -> str:
        return f"{self.spark_home()}/conf/hive-site.xml"
