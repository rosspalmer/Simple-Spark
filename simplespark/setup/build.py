from pathlib import Path
from pprint import pprint
from shutil import rmtree

from simspark.environment import SimpleSparkEnvironment
from simspark.setup.tasks import *


class SetupTaskBuilder:

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
            "java": SetupJavaBin("java", {
                "JAVA_HOME": f"{env.libs_path}/jdk-{env.config.get_package_version('java')}",
                "PATH": "$PATH:$JAVA_HOME/bin"}),
            "scala": SetupJavaBin("scala", {
                "SCALA_HOME": env.package_home_directory('scala'),
                "PATH": "$PATH:$SCALA_HOME/bin"}),
            "spark": SetupJavaBin("spark", {
                "SPARK_HOME": env.package_home_directory('spark'),
                "PATH": "$PATH:$SPARK_HOME/bin"}),
            "setup_driver": SetupDriverConfig(),
            "setup_envs": SetupEnvsScript(),
        }

        optional_tasks: dict[str, SetupTask] = {
            "setup_metastore": SetupHiveMetastore(),
            "setup_jars": SetupMavenJar(),
            "delta": SetupDelta()
        }

        include_optional_tasks: list[str] = []
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
