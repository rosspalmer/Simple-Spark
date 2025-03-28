import os.path
from abc import ABC, abstractmethod

from simplespark.environment.config import SimpleSparkConfig
from simplespark.environment.tasks import (
    BuildTask, SetupWorker, SetupDriver, SetupJavaBin, PrepareConfigFiles,
    ConnectToHiveMetastore, SetupDelta, SetupActivateScript
)
from simplespark.utils.ssh import SSHUtils


class Builder(ABC):

    def __init__(self, config: SimpleSparkConfig, host: str):
        self.config = config
        self.host = host

    def run(self):

        tasks = self._generate_build_tasks()

        for task in tasks:
            print(f'Starting build task: {task.name()}')
            task.run(self.config)


    @abstractmethod
    def _generate_build_tasks(self) -> list[BuildTask]:
        pass

    def _generate_core_tasks(self) -> list[BuildTask]:
        tasks = [
            SetupJavaBin('java'),
            SetupJavaBin('scala'),
            SetupJavaBin('spark'),
            PrepareConfigFiles(self.host)
        ]
        return tasks

    def _generate_optional_tasks(self) -> list[BuildTask]:

        tasks = list()

        # FIXME do we need to install this on workers?
        if self.config.jdbc_drivers:
            tasks.append(None)  # TODO
        # FIXME do we need to install this on workers?
        if self.config.metastore_config:
            tasks.append(ConnectToHiveMetastore())
        # FIXME do we need to install this on workers?
        if "delta" in self.config.packages:
            tasks.append(SetupDelta())

        return tasks


class LocalBuilder(Builder):

    def _generate_build_tasks(self) -> list[BuildTask]:

        tasks = self._generate_core_tasks()
        tasks.append(SetupDriver())

        worker_config = self.config.get_worker_config(self.config.driver.host)
        assert worker_config is not None
        tasks.append(SetupWorker(worker_config))

        tasks.extend(self._generate_optional_tasks())
        tasks.append(SetupActivateScript())

        return tasks


class StandaloneDriverBuilder(Builder):

    def _generate_build_tasks(self) -> list[BuildTask]:

        tasks = self._generate_core_tasks()
        tasks.append(SetupDriver())

        tasks.extend(self._generate_optional_tasks())
        tasks.append(SetupActivateScript())

        return tasks


class StandaloneWorkerBuilder(Builder):

    def _generate_build_tasks(self) -> list[BuildTask]:

        tasks = self._generate_core_tasks()

        worker_config = self.config.get_worker_config(self.host)
        tasks.append(SetupWorker(worker_config))

        tasks.append(SetupActivateScript())

        return tasks


def build_home(config: SimpleSparkConfig):

    create_dirs = [
        config.simplespark_home,
        config.activate_script_directory,
        config.simplespark_bin_directory,
        config.simplespark_environment_directory,
        config.simplespark_libs_directory
    ]

    for directory in create_dirs:
        if not os.path.exists(directory):
            print(f'Creating directory {directory}')
            os.makedirs(directory)

    with open(config.bash_profile_file, 'a') as f:
        f.write(f"\nexport SIMPLESPARK_HOME={config.simplespark_home}")
        f.write(f"\nexport PATH=$PATH:{config.activate_script_directory}")


def build_environment(config: SimpleSparkConfig):

    if config.mode == 'local':
        builder = LocalBuilder(config, 'localhost')
        builder.run()

    elif config.mode == 'standalone':

        print(f'Building driver: {config.driver.host}')
        builder = StandaloneDriverBuilder(config, config.driver.host)
        builder.run()

        worker_hosts = [w.host for w in config.workers if w.host != config.driver.host]
        print(f'Found {len(worker_hosts)} worker hosts')

        for worker_host in worker_hosts:
            print(f'Building worker over SSH: {worker_host}')
            build_worker_via_ssh(config, worker_host)

    else:

        raise Exception(f'Unknown setup type: {config.mode}')


def build_worker(config: SimpleSparkConfig, host: str):

    if config.mode == 'standalone':
        StandaloneWorkerBuilder(config, host).run()
    else:
        raise RuntimeError(f'Unsupported setup_type: {config.mode}')


def build_worker_via_ssh(config: SimpleSparkConfig, host: str):

    ssh = SSHUtils(host)

    # Make SIMPLESPARK_HOME directory for copying over files
    ssh.create_directory(config.simplespark_home)

    # Make binary directory and download simplespark binary
    # FIXME need to make link dynamic, not hardcoded
    binary_name = f"simplespark_0.2.3"
    ssh.create_directory(config.simplespark_bin_directory)
    binary_download = f"https://github.com/rosspalmer/Simple-Spark/releases/download/v0.2.3/{binary_name}"

    print('wget command')
    # Make sure to set umask for executable permission

    ssh.run(f"wget {binary_download} -O {config.simplespark_bin_directory}/{binary_name}")
    ssh.run(f"chmod +x {config.simplespark_bin_directory}/{binary_name}")

    simplespark_binary_call = f"{config.simplespark_bin_directory}/{binary_download.split('/')[-1]}"

    # Copy over config json from driver to worker
    environment_directory = f"{config.simplespark_environment_directory}/{config.name}"
    ssh.create_directory(config.simplespark_environment_directory)
    ssh.create_directory(environment_directory)
    config_file_path = f'{environment_directory}/{config.name}.json'
    ssh.copy(config_file_path, config_file_path)

    # Copy over packages from driver to worker
    ssh.create_directory(config.simplespark_libs_directory)
    for package in config.packages:
        package_path = config.get_package_home_directory(package.name)
        if os.path.exists(package_path):
            ssh.create_directory(f"{config.simplespark_libs_directory}/{package.name}")
            package_directory = config.get_package_home_directory(package.name)
            print(f'Copying over {package} to {package_directory}')
            ssh.copy_directory(package_directory, package_directory)
        else:
            print(f'Skipping, package {package.name}:{package.version} does not exist in libs folder')

    # Run build `worker` command on machine
    stdin, stdout, stderr = ssh.run(f'{simplespark_binary_call} worker {config.name} {host}')

    print(stdout.readlines())
    print(stderr.readlines())