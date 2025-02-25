from pathlib import Path
from pprint import pprint
from shutil import rmtree

from simplespark.environment.env import SimpleSparkEnvironment
from simplespark.cluster.tasks import *


class SetupBuilder:

    def __init__(self, env: SimpleSparkEnvironment):

        self.env = env

        self.OPTIONAL_TASKS: list[SetupTask] = [
            SetupEnvsScript(),
            SetupHiveMetastore(),
            DownloadJDBCDrivers(),
            SetupDelta()
        ]
        self._optional: dict[str, SetupTask] = {t.name(): t for t in self.OPTIONAL_TASKS}

    def get_optional_task(self, task_name: str) -> SetupTask:
        return self._optional[task_name]

    def run(self):

        print("Setup environment config:")
        pprint(self.env.config.__dict__)

        print(f"SimpleSpark HOME directory: {self.env.simple_home}")
        simple_home = Path(self.env.simple_home)
        if not simple_home.exists():
            simple_home.mkdir()
        rmtree(self.env.libs_path, ignore_errors=True)

        tasks: list[SetupTask] = [
            SetupJavaBin("java"),
            SetupJavaBin("scala"),
            SetupJavaBin("spark"),
            SetupDriverConfig()
        ]

        def add_optional_task(task_name: str):
            tasks.append(self.get_optional_task(task_name))

        if self.env.config.setup_type != 'local':
            add_optional_task('cluster-envs')
        if self.env.config.jdbc_drivers:
            add_optional_task('cluster-jars')
        if self.env.config.metastore_config:
            add_optional_task("cluster-metastore")
        if "delta" in self.env.config.packages:
            add_optional_task("cluster-delta")

        # Setup activation script at very end to indicate rest of install completed successfully
        tasks.append(SetupActivateScript())

        for task in tasks:
            print(f'Running task {task.name()}')
            task.run(self.env)
