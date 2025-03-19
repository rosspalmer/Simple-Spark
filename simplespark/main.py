import os
import socket

import typer

from simplespark.environment.build import build_environment, build_worker, build_home
from simplespark.environment.config import SimpleSparkConfig
from simplespark.environment.templates import Templates
from simplespark.utils.shell import ShellManager

app = typer.Typer()


@app.command()
def build(config_paths: str):

    config_files: list[str] = config_paths.split(',')
    config = SimpleSparkConfig.read(*config_files)

    simplespark_home = os.environ.get("SIMPLESPARK_HOME", "")
    if config.simplespark_home != simplespark_home:
        print('Setting up new SIMPLESPARK_HOME directory')
        build_home(config)

    config.write(f'{config.simplespark_home}/config/{config.name}.json')

    print('Setup simplespark environment')
    build_environment(config)

    print(f'Run `source {config.name}` to activate environment')


@app.command()
def run(name: str, code_directory: str, main_file: str):

    config = SimpleSparkConfig.get_simplespark_config(name)

    # Use ShellManager to either run locally or remotely depending on config
    shell = ShellManager(config)

    # Identify name of code directory using folder name
    code_name = code_directory.split("/")[-1]
    code_destination = f"{config.simplespark_home}/archive/{code_name}.zip"

    print(f"Packaging code at {code_directory}")
    print(f"Copying code to {code_destination}")
    shell.archive_and_copy(code_directory, code_destination)

    print(f"Running file {main_file}")
    shell.spark_submit_python(main_file, code_destination)


@app.command()
def start(name: str):
    config = SimpleSparkConfig.get_simplespark_config(name)
    if config.mode == 'local':
        local_hostname = socket.gethostname()
        os.system("bash $SPARK_HOME/sbin/start-master.sh")
        os.system(f"bash $SPARK_HOME/sbin/start-worker.sh spark://{local_hostname}:7077")
    else:
        os.system("bash $SPARK_HOME/sbin/start-all.sh")

    print(f"Spark Cluster UI: http://{config.driver.host}:8080")

    if config.driver.connect_server:
        os.system("bash $SPARK_HOME/sbin/start-connect-server.sh")
        print(f"Started Spark Connect server on {config.driver.host}")

    if config.driver.thrift_server:
        os.system("bash $SPARK_HOME/sbin/start-thriftserver.sh")
        print(f"Started JDBC/ODBC Thrift server on {config.driver.host}")


@app.command()
def stop(name: str):
    config = SimpleSparkConfig.get_simplespark_config(name)
    if config.mode == 'local':
        os.system("bash $SPARK_HOME/sbin/stop-master.sh")
        os.system("bash $SPARK_HOME/sbin/stop-worker.sh localhost")
    else:
        os.system("bash $SPARK_HOME/sbin/stop-all.sh")

    if config.driver.connect_server:
        os.system("bash $SPARK_HOME/sbin/stop-connect-server.sh")
        print(f"Stopped Spark Connect server on {config.driver.host}")

    if config.driver.thrift_server:
        os.system("bash $SPARK_HOME/sbin/stop-thriftserver.sh")
        print(f"Stopped JDBC/ODBC Thrift server on {config.driver.host}")


@app.command()
def template(template_type: str, write_path: str):

    print(f'Create {template_type} template and write to path: {write_path}')

    template_config = Templates.generate(template_type)
    template_config.write(write_path)


@app.command()
def worker(simplespark_config_path: str, worker_host: str):

    config = SimpleSparkConfig.read(simplespark_config_path)

    simplespark_home = os.environ.get("SIMPLESPARK_HOME", None)
    if config.simplespark_home != simplespark_home:
        print(f'Setting up new SIMPLESPARK_HOME directory {config.simplespark_home}')
        build_home(config)

    print(f'Setup simplespark environment on worker {worker_host}')
    build_worker(config, worker_host)
