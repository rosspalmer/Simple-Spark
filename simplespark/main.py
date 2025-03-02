import os

import typer

from simplespark.environment.build import build_environment, build_worker, build_home
from simplespark.environment.config import SimpleSparkConfig
from simplespark.environment.templates import Templates

app = typer.Typer()


@app.command()
def activate(environment: str):

    simplespark_home = os.environ.get("SIMPLESPARK_HOME", None)
    if simplespark_home is None:
        raise Exception("SIMPLESPARK_HOME environment variable not set, need to run `build` first")

    activate_script_path = f"{simplespark_home}/activate/{environment}.sh"

    if not os.path.exists(activate_script_path):
        raise Exception(f"Activation script not found, need to run `build` first: {activate_script_path}")


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
def worker(simplespark_config_path: str, worker_host: str):

    config = SimpleSparkConfig.read(simplespark_config_path)

    simplespark_home = os.environ.get("SIMPLESPARK_HOME", None)
    if config.simplespark_home != simplespark_home:
        print(f'Setting up new SIMPLESPARK_HOME directory {config.simplespark_home}')
        build_home(config)

    print(f'Setup simplespark environment on worker {worker_host}')
    build_worker(config, worker_host)


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

    template_config = Templates.generate(template_type)
    template_config.write(write_path)
