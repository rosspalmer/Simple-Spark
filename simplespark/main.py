import json
import os
import typer

from simplespark.environment.config import SimpleSparkConfig
from simplespark.environment.env import SimpleSparkEnvironment
from simplespark.environment.build import EnvironmentBuilder
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
def install(simple_spark_home_directory: str, bash_file_path: str):

    # TODO add default bash_file_path pointed to user's .bashrc

    if not os.path.exists(simple_spark_home_directory):
        print(f'SIMPLE_SPARK_HOME directory not found, creating: {simple_spark_home_directory}')
        os.makedirs(simple_spark_home_directory)

    with open(bash_file_path, 'a') as f:
        f.write(f"\nexport SIMPLE_SPARK_HOME={simple_spark_home_directory}")
        f.write(f"\nexport PATH=$PATH:{simple_spark_home_directory}/activate")


@app.command()
def build(config_paths: str):

    if os.environ.get("SIMPLE_SPARK_HOME") is None:
        raise Exception("SIMPLE_SPARK_HOME environment variable not set, need to `install` first")

    print(f'Loading simplespark config file(s): {config_paths}')
    config_files: list[str] = config_paths.split(',')
    config = SimpleSparkConfig.read(*config_files)

    print('Initializing simplespark environment')
    env = SimpleSparkEnvironment(config, local_host)

    print('Setup simplespark environment on hardware')
    EnvironmentBuilder(env).run_on_host()

    print(f'Run `source {env.config.name}` to activate environment')


def build_worker(config_json: str):
    config = json.loads(config_json)
    env = SimpleSparkEnvironment(config)

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
