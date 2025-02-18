from abc import ABC, abstractmethod
import os
import tarfile

from urllib.request import urlretrieve

from simplespark.config import ResourceConfig, JdbcConfig
from simplespark.environment import SimpleSparkEnvironment
from simplespark.utils.maven import MavenDownloader


class SetupTask(ABC):

    @abstractmethod
    def run(self, env: SimpleSparkEnvironment):
        pass


class SetupJavaBin(SetupTask):

    def __init__(self, package: str,  env_variables: dict[str, str]):
        self.package = package
        self.env_variables = env_variables

    def run(self, env: SimpleSparkEnvironment):

        download_url = env.full_package_url(self.package)
        download_path = f"{env.config.simple_home}/{env.archive_name(self.package)}"
        lib_path = f"{env.libs_path}/{env.package_names[self.package]}"

        print(f"Downloading {self.package} binary from:")
        print(download_url)

        urlretrieve(download_url, download_path)

        lib_tarfile = tarfile.open(download_path, "r")
        lib_tarfile.extractall(env.libs_path)
        lib_tarfile.close()
        os.remove(download_path)

        print(f"Updating profile script at: {env.config.profile_path}")
        if not os.path.isfile(env.config.profile_path):
            raise FileNotFoundError(f"Profile script not found: {env.config.profile_path}")

        # TODO overwrite if exports already existing in file
        with open(env.config.profile_path, 'a') as file:
            for variable_name, variable_value in self.env_variables.items():
                file.writelines(f'export {variable_name}={variable_value}\n')


class SetupMavenJar(SetupTask):

    def run(self, env: SimpleSparkEnvironment):

        for name, jdbc_driver in env.config.jdbc_drivers.items():
            print(f'Setting up JDBC for {name}')
            MavenDownloader.download_jar(jdbc_driver, env.spark_jars_path())


class SetupDelta(SetupTask):

    def run(self, env: SimpleSparkEnvironment):

        print("Adding Delta libraries to spark_defaults.conf file")

        # TODO overwrite delta configs instead of appending
        with open(env.spark_config_path(), 'a') as spark_config_file:
            spark_config_file \
                .write(f"spark.jars.packages io.delta:delta-spark_2.12:{env.config.get_package_version('delta')}\n")
            spark_config_file.write("spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension\n")
            spark_config_file.write("spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog\n")


class SetupDriverConfig(SetupTask):

    def run(self, env: SimpleSparkEnvironment):

        print(f"Setup driver config at {env.spark_config_path()}")

        # TODO overwrite delta configs instead of appending
        with open(env.spark_config_path(), 'a') as spark_config_file:

            if env.config.driver and env.config.driver.cores:
                spark_config_file.write(f"spark.driver.cores {env.config.driver.cores}\n")

            if env.config.driver and env.config.driver.memory:
                spark_config_file.write(f"spark.driver.memory {env.config.driver.memory}\n")

            if env.config.executor_memory:
                spark_config_file.write(f"spark.executor.memory {env.config.executor_memory}\n")

            if env.config.derby_path:
                spark_config_file.write("spark.driver.extraJavaOptions "
                                        f"-Dderby.system.home={env.config.derby_path}")

            if env.config.warehouse_path:
                spark_config_file.write(f"spark.sql.warehouse.dir {env.config.warehouse_path}\n")


class SetupEnvsScript(SetupTask):

    def run(self, env: SimpleSparkEnvironment):

        print(f"Setup spark-env.sh bash script at {env.spark_env_sh_path()}")

        worker_info: ResourceConfig | None = None
        for worker_config in env.config.workers:
            if worker_config.host == env.local_host:
                worker_info = worker_config
                break

        with open(env.spark_env_sh_path(), 'a') as env_sh_file:

            print("Writing required environment variables")
            env_sh_file.write(f'export SPARK_LOCAL_IP={env.local_host}\n')
            env_sh_file.write(f'export SPARK_HOST_IP={env.config.driver.host}\n')

            if worker_info:
                print(f"Found worker spec: {worker_info}")
                env_sh_file.write(f'export SPARK_WORKER_CORES={worker_info.cores}\n')
                env_sh_file.write(f'export SPARK_WORKER_MEMORY={worker_info.memory}\n')
                env_sh_file.write(f'export SPARK_WORKER_INSTANCES={worker_info.instances}\n')


class SetupHiveMetastore(SetupTask):

    @staticmethod
    def generate_hive_site_xml(env: SimpleSparkEnvironment) -> str:

        config: JdbcConfig = env.config.metastore_config

        xml = f"""<configuration>
        <property>
            <name>javax.jdo.option.ConnectionURL</name>
            <value>{config.get_url()}</value>
        </property>
        <property>
            <name>javax.jdo.option.ConnectionDriverName</name>
            <value>{config.jdbc_driver}</value>
        </property>
        <property>
            <name>javax.jdo.option.ConnectionUserName</name>
            <value>{config.db_user}</value>
        </property>
        <property>
            <name>javax.jdo.option.ConnectionPassword</name>
            <value>{config.db_pass}</value>
        </property>
        <property>
            <name>hive.metastore.warehouse.dir</name>
            <value>{env.config.warehouse_path}</value>
        </property>
        <property>
            <name>hive.metastore.db.type</name>
            <value>{config.db_type}</value>
        </property>
        </configuration>
        """

        return xml

    def run(self, env: SimpleSparkEnvironment):

        config = env.config.metastore_config
        if config is None:
            raise Exception("Must specify `metastore_config` to setup Hive metastore")

        supported_types = {t for t in env.config.jdbc_drivers.keys()}
        if config.db_type not in supported_types:
            raise Exception(
                f"Metastore db_type '{config.db_type}' not a supported type: [{','.join(supported_types)}]"
            )

        with open(env.hive_config_path(), "w") as hc:
            hc.write(self.generate_hive_site_xml(env))
