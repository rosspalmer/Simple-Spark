import json
import os
import tempfile

import typer

from simplespark.environment.build import build_environment, build_worker
from simplespark.environment.config import SimpleSparkConfig
from simplespark.environment.templates import Templates

app = typer.Typer()


@app.command()
def activate(environment: str):

    simple_spark_home = os.environ.get("SIMPLE_SPARK_HOME", None)
    if simple_spark_home is None:
        raise Exception("SIMPLE_SPARK_HOME environment variable not set, need to run `install` first")

    activate_script_path = f"{simple_spark_home}/activate/{environment}.sh"

    if not os.path.exists(activate_script_path):
        raise Exception(f"Activation script not found, need to run `build` first: {activate_script_path}")

@app.command()
def upload(config_paths: str):

    config_files: list[str] = config_paths.split(',')
    config = SimpleSparkConfig.read(*config_files)

    simple_spark_home = os.environ.get("SIMPLESPARK_HOME", "")
    if config.simple_home != simple_spark_home:
        print('Setting up new SIMPLESPARK_HOME variable, need to reset terminal to take effect')
        print(f'or use `source {config.bash_file_path}`')

        if not os.path.exists(config.simple_home):
            print(f'Created empty HOME folder: {config.simple_home}')
            os.makedirs(config.simple_home)

        with open(config.bash_file_path, 'a') as f:
            f.write(f"\nexport SIMPLESPARK_HOME={config.simple_home}")
            f.write(f"\nexport PATH=$PATH:{config.simple_home}/activate")

    config.write(f'{config.simple_home}/config/{config.name}.json')


@app.command()
def build(environment: str, worker_host: str = None):

    simplespark_home = os.environ.get("SIMPLE_SPARK_HOME")
    if simplespark_home is None:
        raise Exception("SIMPLE_SPARK_HOME environment variable not set")

    config = SimpleSparkConfig.read(f'{simplespark_home}/config/{environment}.json')

    if worker_host is None:
        print('Setup simplespark environment')
        build_environment(config)
    else:
        print(f'Setup simplespark environment on worker {worker_host}')
        build_worker(config, worker_host)

    print(f'Run `source {config.name}` to activate environment')

@app.command()
def start(environment: str):
    activate(environment)
    os.system(f'sh $SPARK_HOME/sbin/start-all.sh')


@app.command()
def stop(environment: str):
    activate(environment)
    os.system(f'sh $SPARK_HOME/sbin/stop-all.sh')


@app.command()
def template(template_type: str, write_path: str):

    print(f'Create template type: {template_type} and write to path: {write_path}')

    template = Templates.generate(template_type)
    template_json = template.get_as_json()

    with open(write_path, 'w') as f:
        json.dump(template_json, f, indent=2)
