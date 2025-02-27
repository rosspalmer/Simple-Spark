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
            SetupWorker(),
            SetupHiveMetastore(),
            DownloadJDBCDrivers(),
            SetupDelta()
        ]

    @staticmethod
    def run(env: SimpleSparkEnvironment, host: str):

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

        tasks: list[SetupTask] = [
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
        tasks.extend([
            AddWorkersFile(),
            SetupActivateScript()
        ])

        for task in tasks:
            print(f'Running build task {task.name()}')
            task.run(self.env)
