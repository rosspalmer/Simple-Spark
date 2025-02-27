from pathlib import Path
from pprint import pprint
from shutil import rmtree

from simplespark.environment.env import SimpleSparkEnvironment
from simplespark.cluster.tasks import *


class SetupBuilder:

    def __init__(self, env: SimpleSparkEnvironment):

        self.env = env

        self.OPTIONAL_TASKS: list[SetupTask] = [
            SetupDriver(),
            SetupEnvsScript(),
            SetupHiveMetastore(),
            DownloadJDBCDrivers(),
            SetupDelta()
        ]
        self._optional: dict[str, SetupTask] = {t.name(): t for t in self.OPTIONAL_TASKS}

    def get_optional_task(self, task_name: str) -> SetupTask:
        return self._optional[task_name]

    def run(self, host: str):

        print("Setup environment config:")
        pprint(self.env.config.__dict__)

        print(f"SimpleSpark HOME directory: {self.env.simple_home}")
        simple_home = Path(self.env.simple_home)
        if not simple_home.exists():
            simple_home.mkdir()
        rmtree(self.env.libs_path, ignore_errors=True)

        print(f'Building on host "{host}"')

        driver_config = None
        if self.env.is_driver(host):
            driver_config = self.env.config.driver
            print(f'Setting up host as driver: {driver_config}')

        worker_config = self.env.get_worker_config(host)
        if worker_config is not None:
            print(f'Setting up host as worker config: {worker_config}')

        tasks: list[SetupTask] = [
            SetupJavaBin("java"),
            SetupJavaBin("scala"),
            SetupJavaBin("spark"),
        ]

        def add_optional_task(task_name: str):
            tasks.append(self.get_optional_task(task_name))

        if driver_config is not None:
            add_optional_task('setup-driver')
        if self.env.config.setup_type != 'local':
            add_optional_task('setup-envs')
        if self.env.config.jdbc_drivers:
            add_optional_task('setup-jars')
        if self.env.config.metastore_config:
            add_optional_task("setup-metastore")
        if "delta" in self.env.config.packages:
            add_optional_task("setup-delta")

        # Add script tasks very end to indicate rest of install completed successfully
        tasks.extend([
            AddWorkersFile(),
            SetupActivateScript()
        ])

        for task in tasks:
            print(f'Running build task {task.name()}')
            task.run(self.env)
