import json
import os
import typer

from simplespark.environment.config import SimpleSparkConfig
from simplespark.environment.env import SimpleSparkEnvironment
from simplespark.setup.build import SetupBuilder
from simplespark.templates import Templates

app = typer.Typer()


@app.command()
def activate(environment: str):

    simple_spark_home = os.environ.get("SIMPLE_SPARK_HOME", None)
    if simple_spark_home is None:
        raise Exception("SIMPLE_SPARK_HOME environment variable not set, need to run `set_home` first")

    activate_script_path = f"{simple_spark_home}/envs/{environment}.sh"

    if not os.path.exists(activate_script_path):
        raise Exception(f"Activation script not found, need to run `setup` first: {activate_script_path}")

@app.command()
def info():
    print(f'TODO')

@app.command()
def set_home(simple_spark_home_directory: str, bash_file_path: str = '~/.bashrc'):

    if not os.path.exists(simple_spark_home_directory):
        os.makedirs(simple_spark_home_directory)

    with open(bash_file_path) as f:
        f.write(f"export SIMPLE_SPARK_HOME={simple_spark_home_directory}")


@app.command()
def setup(config_paths: str, local_host: str = ''):

    print(f'Loading simplespark config file(s): {config_paths}')
    config_files: list[str] = config_paths.split(',')
    config = SimpleSparkConfig.read(*config_files)

    print('Initializing simplespark environment')
    env = SimpleSparkEnvironment(config, local_host)

    print('Setup simplespark environment on hardware')
    SetupBuilder(env).run()


@app.command()
def template(template_type: str, write_path: str):

    print(f'Create template type: {template_type} and write to path: {write_path}')

    template = Templates.generate(template_type)
    template_json = template.get_as_json()

    with open(write_path, 'w') as f:
        json.dump(template_json, f, indent=2)
