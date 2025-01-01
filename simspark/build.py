from pathlib import Path
import shutil
from pprint import pprint

from simspark.setup import *
from simspark.environment import SimpleSparkEnvironment


class SimpleBuild:
    pass
    #
    # @staticmethod
    # def run(env: SimpleEnvironment):
    #
    #     print("Building environment:")
    #     pprint(env.config.__dict__)
    #
    #     print(f"Simple-Delta HOME directory: {env.config.simple_home}")
    #     simple_home = Path(env.config.simple_home)
    #     if not simple_home.exists():
    #         simple_home.mkdir()
    #     shutil.rmtree(env.libs_path, ignore_errors=True)
    #
    #     required_tasks: Dict[str, SetupTask] = {
    #         "install_java": SetupJavaBin("java", {"JAVA_HOME":
    #                                               f"{env.libs_path}/jdk-{env.config.get_package_version('java')}",
    #                                      "PATH": "$PATH:$JAVA_HOME/bin"}),
    #         "install_scala": SetupJavaBin("scala", {"SCALA_HOME": env.package_home_directory('scala'),
    #                                       "PATH": "$PATH:$SCALA_HOME/bin"}),
    #         "install_spark": SetupJavaBin("spark", {"SPARK_HOME": env.package_home_directory('spark'),
    #                                       "PATH": "$PATH:$SPARK_HOME/bin"}),
    #         "setup_master": SetupMasterConfig(),
    #         "setup_envs": SetupEnvsScript(),
    #     }
    #
    #     optional_tasks: Dict[str, SetupTask] = {
    #         "setup_metastore": SetupHiveMetastore(),
    #         "install_delta": SetupDelta(),
    #     }
    #
    #     include_optional_tasks: List[str] = []
    #     if env.config.metastore_config:
    #         include_optional_tasks.append("setup_metastore")
    #     if "delta" in env.config.packages:
    #         include_optional_tasks.append("install_delta")
    #
    #     for task_name, task in required_tasks.items():
    #         task.run(env)
    #
    #     for task_name in include_optional_tasks:
    #         task = optional_tasks[task_name]
    #         task.run(env)
