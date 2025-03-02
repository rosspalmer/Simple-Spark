import shutil
from abc import ABC, abstractmethod
import os
import tarfile

from urllib.request import urlretrieve

from simplespark.environment.config import SimpleSparkConfig, JdbcConfig, WorkerConfig
from simplespark.utils.maven import MavenDownloader
from simplespark.utils.ssh import SSHUtils


class BuildTask(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def run(self, config: SimpleSparkConfig):
        pass


class SetupHome(BuildTask):

    def name(self) -> str:
        return "setup-home"

    def run(self, config: SimpleSparkConfig):
        pass

class SetupJavaBin(BuildTask):

    def __init__(self, package: str):
        self.package = package

    def name(self) -> str:
        return f"setup-{self.package}-bin"

    def run(self, config: SimpleSparkConfig):

        package_config = config.get_package_config(self.package)
        package_directory = f'{config.simplespark_libs_directory}/{self.package}'
        if not os.path.exists(package_directory):
            os.makedirs(package_directory)

        download_url = package_config.package_download_url
        download_path = f"{config.simplespark_home}/{package_config.package_file_name}"

        if not os.path.exists(config.get_package_home_directory(self.package)):

            print(f"Downloading {self.package} binary from:")
            print(download_url)

            urlretrieve(download_url, download_path)

            lib_tarfile = tarfile.open(download_path, "r")
            lib_tarfile.extractall(config.simplespark_libs_directory)

            # Assumes single folder in extract with package extracted within
            extracted_folder_path = f"{config.simplespark_libs_directory}/{lib_tarfile.getnames()[0]}"

            print(f"Move unpacked lib from {extracted_folder_path} to {config.get_package_home_directory(self.package)}")
            if not os.path.exists(f"{config.simplespark_libs_directory}/{self.package}"):
                os.makedirs(f"{config.simplespark_libs_directory}/{self.package}")

            shutil.copytree(extracted_folder_path, config.get_package_home_directory(self.package))

            lib_tarfile.close()
            os.remove(download_path)
            shutil.rmtree(extracted_folder_path)

        else:

            print(f"Package version already downloaded at {config.get_package_home_directory(self.package)}")


class PrepareConfigFiles(BuildTask):

    def __init__(self, host: str):
        self.host = host

    def name(self) -> str:
        return "prepare-config-files"

    def run(self, config: SimpleSparkConfig):

        print(f"Setup spark-env.sh bash script at {config.spark_env_sh_path}")
        with open(config.spark_env_sh_path, 'w') as env_sh_file:

            print("Writing required environment variables")
            env_sh_file.write(f'export SPARK_LOCAL_IP={self.host}\n')
            env_sh_file.write(f'export SPARK_HOST_IP={config.driver.host}\n')


class DownloadJDBCDrivers(BuildTask):

    def name(self) -> str:
        return "download-jdbc-drivers"

    def run(self, config: SimpleSparkConfig):

        for name, jdbc_driver in config.jdbc_drivers.items():
            print(f'Setting up JDBC for {name}')
            MavenDownloader.download_jar(jdbc_driver, config.spark_jars_path)


class SetupDelta(BuildTask):

    def name(self) -> str:
        return "setup-delta"

    def run(self, config: SimpleSparkConfig):

        print("Adding Delta libraries to spark_defaults.conf file")

        # TODO overwrite delta configs instead of appending
        with open(config.spark_config_path, 'a') as spark_config_file:
            spark_config_file \
                .write(f"spark.jars.packages io.delta:delta-spark_2.12:{config.get_package_version('delta')}\n")
            spark_config_file.write("spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension\n")
            spark_config_file.write("spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog\n")


class SetupDriver(BuildTask):

    def name(self) -> str:
        return "setup-driver"

    def run(self, config: SimpleSparkConfig):

        print(f"Setup driver config at {config.spark_config_path}")

        with open(config.spark_config_path, 'a') as spark_config_file:

            if config.driver and config.driver.cores:
                spark_config_file.write(f"spark.driver.cores {config.driver.cores}\n")

            if config.driver and config.driver.memory:
                spark_config_file.write(f"spark.driver.memory {config.driver.memory}\n")

            if config.executor_memory:
                spark_config_file.write(f"spark.executor.memory {config.executor_memory}\n")

            if config.derby_path:
                spark_config_file.write("spark.driver.extraJavaOptions "
                                        f"-Dderby.system.home={config.derby_path}")

            if config.warehouse_path:
                spark_config_file.write(f"spark.sql.warehouse.dir {config.warehouse_path}\n")

            # Add `conf/workers` file if running in standalone mode
            if config.setup_type == 'standalone':

                workers_file_path = f'{config.spark_config_path}/workers'

                with open(workers_file_path, "w") as wf:
                    print(f'Creating {workers_file_path} file')

                    for w in config.workers:
                        print(f'Adding worker: {w.host}')
                        wf.write(w.host + '\n')


class SetupWorker(BuildTask):

    def __init__(self, worker_config: WorkerConfig):
        self.worker_config = worker_config

    def name(self) -> str:
        return "setup-worker"

    def run(self, config: SimpleSparkConfig):

        print('Setting up worker configuration')

        with open(config.spark_env_sh_path, 'a') as env_sh_file:

            env_sh_file.write(f'export SPARK_WORKER_CORES={self.worker_config.cores}\n')
            env_sh_file.write(f'export SPARK_WORKER_MEMORY={self.worker_config.memory}\n')
            env_sh_file.write(f'export SPARK_WORKER_INSTANCES={self.worker_config.instances}\n')


class ConnectToHiveMetastore(BuildTask):

    def name(self) -> str:
        return "connect-to-hive-metastore"

    @staticmethod
    def generate_hive_site_xml(config: SimpleSparkConfig) -> str:

        jdbc_config: JdbcConfig = config.metastore_config

        xml = f"""<configuration>
        <property>
            <name>javax.jdo.option.ConnectionURL</name>
            <value>{jdbc_config.get_url()}</value>
        </property>
        <property>
            <name>javax.jdo.option.ConnectionDriverName</name>
            <value>{jdbc_config.jdbc_driver}</value>
        </property>
        <property>
            <name>javax.jdo.option.ConnectionUserName</name>
            <value>{jdbc_config.db_user}</value>
        </property>
        <property>
            <name>javax.jdo.option.ConnectionPassword</name>
            <value>{jdbc_config.db_pass}</value>
        </property>
        <property>
            <name>hive.metastore.warehouse.dir</name>
            <value>{config.warehouse_path}</value>
        </property>
        <property>
            <name>hive.metastore.db.type</name>
            <value>{jdbc_config.db_type}</value>
        </property>
        </configuration>
        """

        return xml

    def run(self, config: SimpleSparkConfig):

        if config.metastore_config is None:
            raise Exception("Must specify `metastore_config` to cluster Hive metastore")

        supported_types = {t for t in config.jdbc_drivers.keys()}
        if config.metastore_config.db_type not in supported_types:
            raise Exception(
                f"Metastore db_type '{config.metastore_config.db_type}' "
                f"not a supported type: [{','.join(supported_types)}]"
            )

        with open(config.hive_config_path, "w") as hc:
            hc.write(self.generate_hive_site_xml(config))


class SetupActivateScript(BuildTask):

    def name(self) -> str:
        return "setup-activate-script"

    def run(self, config: SimpleSparkConfig):

        new_env_variables = {
            "JAVA_HOME": config.get_package_home_directory('java'),
            "SCALA_HOME": config.get_package_home_directory('scala'),
            "SPARK_HOME": config.get_package_home_directory('spark')
        }
        new_path_additions = ["$JAVA_HOME/bin", "$SCALA_HOME/bin", "$SPARK_HOME/bin"]

        if not os.path.exists(config.activate_script_directory):
            print("Activate script directory does not exist")
            print(f"Creating: {config.activate_script_directory}")
            os.mkdir(config.activate_script_directory)

        with open(config.activate_script_path, 'w') as f:

            # Add `export` command for each new environment variable
            for k, v in new_env_variables.items():
                f.write(f'\nexport {k}="{v}"')

            # Add additions to PATH variable by merging into single `export` command
            f.write(f"\nexport PATH=$PATH:{':'.join(new_path_additions)}")


class SetupWorkerViaSSH(BuildTask):

    def __init__(self, host: str):
        self.host = host

    def name(self) -> str:
        return "setup-worker-via-ssh"

    def run(self, config: SimpleSparkConfig):

        ssh = SSHUtils(self.host)

        # Copy over packages from driver to worker
        for package in config.packages:
            package_directory = config.get_package_home_directory(package.name)
            ssh.copy_directory(package_directory, package_directory)

        # TODO Only run if install is needed
        debug = ssh.run(f'pip install simplespark')
        debug = ssh.run(f'simplespark build {self.host}')
