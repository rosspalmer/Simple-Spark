from pathlib import Path
from pprint import pprint
from shutil import rmtree

from simplespark.environment.env import SimpleSparkEnvironment
from simplespark.setup.tasks import *


class SetupBuilder:

    @staticmethod
    def run(env: SimpleSparkEnvironment):

        print("Syncing environment:")
        pprint(env.config.__dict__)

        print(f"Simple-Delta HOME directory: {env.config.simple_home}")
        simple_home = Path(env.config.simple_home)
        if not simple_home.exists():
            simple_home.mkdir()
        rmtree(env.libs_path, ignore_errors=True)

        required_tasks: dict[str, SetupTask] = {
            "java": SetupJavaBin("java"),
            "scala": SetupJavaBin("scala"),
            "spark": SetupJavaBin("spark"),
            "activate_script": SetupActivateScript(),
            "setup_driver": SetupDriverConfig(),
        }

        optional_tasks: dict[str, SetupTask] = {
            "setup_envs": SetupEnvsScript(),
            "setup_metastore": SetupHiveMetastore(),
            "setup_jars": SetupMavenJar(),
            "delta": SetupDelta()
        }

        include_optional_tasks: list[str] = []

        if env.config.setup_type != 'local':
            include_optional_tasks.append('setup_envs')
        if env.config.jdbc_drivers:
            include_optional_tasks.append('setup_jars')
        if env.config.metastore_config:
            include_optional_tasks.append("setup_metastore")
        if "delta" in env.config.packages:
            include_optional_tasks.append("delta")

        for task_name, task in required_tasks.items():
            print(f'Running task {task_name}')
            task.run(env)

        for task_name in include_optional_tasks:
            print(f'Running task {task_name}')
            task = optional_tasks[task_name]
            task.run(env)
