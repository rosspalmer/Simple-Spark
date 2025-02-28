from abc import ABC, abstractmethod
from multiprocessing.pool import worker
from pathlib import Path
from pprint import pprint
from shutil import rmtree

from simplespark.environment.env import SimpleSparkEnvironment
from simplespark.environment.tasks import BuildTask, SetupWorker, SetupDriver, SetupJavaBin, PrepareConfigFiles, \
    SetupHiveMetastore, SetupDelta, SetupActivateScript
from simplespark.utils.ssh import ShellCommand

class Builder(ABC):

    def __init__(self, env: SimpleSparkEnvironment):
        self.env = env

    def run(self):

        tasks = self._generate_build_tasks()

        for task in tasks:
            print(f'Starting build task: {task.name()}')

    @abstractmethod
    def _generate_build_tasks(self) -> list[BuildTask]:
        pass

    @abstractmethod
    def get_mode(self):
        pass

    def _generate_core_tasks(self) -> list[BuildTask]:
        tasks = [
            PrepareConfigFiles(self.host),
            SetupJavaBin('java'),
            SetupJavaBin('scala'),
            SetupJavaBin('spark')
        ]
        return tasks

    def _generate_optional_tasks(self) -> list[BuildTask]:

        tasks = list()

        # FIXME do we need to install this on workers?
        if self.env.config.jdbc_drivers:
            tasks.append(None)  # TODO
        # FIXME do we need to install this on workers?
        if self.env.config.metastore_config:
            tasks.append(SetupHiveMetastore())
        # FIXME do we need to install this on workers?
        if "delta" in self.env.config.packages:
            tasks.append(SetupDelta())

        return tasks


class LocalBuilder(Builder):

    def get_mode(self) -> str:
        return "local"

    def _generate_build_tasks(self) -> list[BuildTask]:

        tasks = self._generate_core_tasks()
        tasks.append(SetupDriver())

        worker_config = self.env.get_worker_config(self.env.config.driver.host)
        assert worker_config is not None
        tasks.append(SetupWorker(worker_config))

        tasks.extend(self._generate_optional_tasks())
        tasks.append(SetupActivateScript())

        return tasks


class StandaloneBuilder(Builder):

    def get_mode(self) -> str:
        return "standalone"

    def _generate_build_tasks(self) -> list[BuildTask]:

        tasks = self._generate_core_tasks()
        tasks.append(SetupDriver())

        tasks.extend(self._generate_optional_tasks())
        tasks.append(SetupActivateScript())

        worker_config = self.env.get_worker_config(self.env.config.driver.host)
        assert worker_config is not None
        tasks.append(SetupWorker(worker_config))

        tasks.extend(self._generate_optional_tasks())
        tasks.append(SetupActivateScript())




def build_environment(env: SimpleSparkEnvironment):

    builder = None
    if env.config.setup_type == 'local':
        builder = LocalBuilder(env)
    elif env.config.setup_type == 'standalone':
        builder =
    assert builder is not None

    builder.run()

    return None

class EnvironmentBuilder:

    @staticmethod
    def run(env: SimpleSparkEnvironment):

        # Run builder function on host driver machine
        EnvironmentBuilder.run_on_host(env, env.config.driver.host)

        # Run builder function on worker machines via SSH
        for worker in env.config.workers:
            sc = ShellCommand(worker.host)
            sc.run('pip install simplespark')


    @staticmethod
    def run_on_host(env: SimpleSparkEnvironment, host: str):

        print("Setup environment config:")
        pprint(env.config.__dict__)

        print(f"SimpleSpark HOME directory: {env.simple_home}")
        simple_home = Path(env.simple_home)
        if not simple_home.exists():
            simple_home.mkdir()
        rmtree(env.libs_path, ignore_errors=True)

        print(f'Building on host "{host}"')

        driver_config = None
        if env.is_driver(host):
            driver_config = env.config.driver
            print(f'Setting up host as driver: {driver_config}')

        worker_config = env.get_worker_config(host)
        if worker_config is not None:
            print(f'Setting up host as worker config: {worker_config}')

        tasks: list[BuildTask] = [
            PrepareConfigFiles(host),
            SetupJavaBin("java"),
            SetupJavaBin("scala"),
            SetupJavaBin("spark"),
        ]

        if driver_config is not None:
            tasks.append(SetupDriver())
        if worker_config is not None:
            tasks.append(SetupWorker(worker_config))
        # FIXME do we need to install this on workers?
        if env.config.jdbc_drivers:
            tasks.append(None)  # TODO
        # FIXME do we need to install this on workers?
        if env.config.metastore_config:
            tasks.append(SetupHiveMetastore())
        # FIXME do we need to install this on workers?
        if "delta" in env.config.packages:
            tasks.append(SetupDelta())

        # Add script tasks very end to indicate rest of install completed successfully
        tasks.append(SetupActivateScript())

        for task in tasks:
            print(f'Running build task {task.name()}')
            task.run(env)
