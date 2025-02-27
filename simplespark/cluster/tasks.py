import shutil
from abc import ABC, abstractmethod
import os
import tarfile

from urllib.request import urlretrieve

from simplespark.environment.config import JdbcConfig, WorkerConfig
from simplespark.environment.env import SimpleSparkEnvironment
from simplespark.utils.maven import MavenDownloader


class SetupTask(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def run(self, env: SimpleSparkEnvironment):
        pass


class PrepareConfigFiles(SetupTask):

    def __init__(self, host: str):
        self.host = host

    def name(self) -> str:
        return "prepare-config-files"

    def run(self, env: SimpleSparkEnvironment):

        print(f"Setup spark-env.sh bash script at {env.spark_env_sh_path()}")
        with open(env.spark_env_sh_path(), 'w') as env_sh_file:

            print("Writing required environment variables")
            env_sh_file.write(f'export SPARK_LOCAL_IP={self.host}\n')
            env_sh_file.write(f'export SPARK_HOST_IP={env.config.driver.host}\n')


class SetupJavaBin(SetupTask):

    def __init__(self, package: str):
        self.package = package

    def name(self) -> str:
        return f"setup-{self.package}-bin"

    def run(self, env: SimpleSparkEnvironment):

        download_url = env.full_package_url(self.package)
        download_path = f"{env.simple_home}/{env.archive_name(self.package)}"

        print(f"Downloading {self.package} binary from:")
        print(download_url)

        urlretrieve(download_url, download_path)

        lib_tarfile = tarfile.open(download_path, "r")
        lib_tarfile.extractall(env.libs_path)

        extracted_folder_path = f"{env.libs_path}/{lib_tarfile.getnames()[0]}"

        print(f"Move unpacked lib from {extracted_folder_path} to {env.get_package_home_directory(self.package)}")
        if not os.path.exists(env.get_package_libs_directory(self.package)):
            os.makedirs(env.get_package_libs_directory(self.package))

        shutil.copytree(extracted_folder_path, env.get_package_home_directory(self.package))

        lib_tarfile.close()
        os.remove(download_path)
        shutil.rmtree(extracted_folder_path)


class DownloadJDBCDrivers(SetupTask):

    def name(self) -> str:
        return "download-jdbc-drivers"

    def run(self, env: SimpleSparkEnvironment):

        for name, jdbc_driver in env.config.jdbc_drivers.items():
            print(f'Setting up JDBC for {name}')
            MavenDownloader.download_jar(jdbc_driver, env.spark_jars_path())


class SetupDelta(SetupTask):

    def name(self) -> str:
        return "setup-delta"

    def run(self, env: SimpleSparkEnvironment):

        print("Adding Delta libraries to spark_defaults.conf file")

        # TODO overwrite delta configs instead of appending
        with open(env.spark_config_path(), 'a') as spark_config_file:
            spark_config_file \
                .write(f"spark.jars.packages io.delta:delta-spark_2.12:{env.config.get_package_version('delta')}\n")
            spark_config_file.write("spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension\n")
            spark_config_file.write("spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog\n")


class SetupDriver(SetupTask):

    def name(self) -> str:
        return "setup-driver"

    def run(self, env: SimpleSparkEnvironment):

        print(f"Setup driver config at {env.spark_config_path()}")

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

            # Add `conf/workers` file if running in standalone mode
            if env.config.setup_type == 'standalone':

                workers_file_path = f'{env.spark_config_path()}/workers'

                with open(workers_file_path, "w") as wf:
                    print(f'Creating {workers_file_path} file')

                    for w in env.config.workers:
                        print(f'Adding worker: {w.host}')
                        wf.write(w.host + '\n')


class SetupWorker(SetupTask):

    def __init__(self, worker_config: WorkerConfig):
        self.worker_config = worker_config

    def name(self) -> str:
        return "setup-worker"

    def run(self, env: SimpleSparkEnvironment):

        print('Setting up worker configuration')

        with open(env.spark_env_sh_path(), 'a') as env_sh_file:

            env_sh_file.write(f'export SPARK_WORKER_CORES={self.worker_config.cores}\n')
            env_sh_file.write(f'export SPARK_WORKER_MEMORY={self.worker_config.memory}\n')
            env_sh_file.write(f'export SPARK_WORKER_INSTANCES={self.worker_config.instances}\n')


class SetupHiveMetastore(SetupTask):

    def name(self) -> str:
        return "setup-hive-metastore"

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
            raise Exception("Must specify `metastore_config` to cluster Hive metastore")

        supported_types = {t for t in env.config.jdbc_drivers.keys()}
        if config.db_type not in supported_types:
            raise Exception(
                f"Metastore db_type '{config.db_type}' not a supported type: [{','.join(supported_types)}]"
            )

        with open(env.hive_config_path(), "w") as hc:
            hc.write(self.generate_hive_site_xml(env))


class SetupActivateScript(SetupTask):

    def name(self) -> str:
        return "setup-activate-script"

    def run(self, env: SimpleSparkEnvironment):

        new_env_variables = {
            "JAVA_HOME": env.get_package_home_directory('java'),
            "SCALA_HOME": env.get_package_home_directory('scala'),
            "SPARK_HOME": env.get_package_home_directory('spark')
        }
        new_path_additions = ["$JAVA_HOME/bin", "$SCALA_HOME/bin", "$SPARK_HOME/bin"]

        if not os.path.exists(env.get_activate_script_directory()):
            print("Activate script directory does not exist")
            print(f"Creating: {env.get_activate_script_directory()}")
            os.mkdir(env.get_activate_script_directory())

        with open(env.get_activate_script_path(), 'w') as f:

            # Add `export` command for each new environment variable
            for k, v in new_env_variables.items():
                f.write(f'\nexport {k}="{v}"')

            # Add additions to PATH variable by merging into single `export` command
            f.write(f"\nexport PATH=$PATH:{':'.join(new_path_additions)}")
