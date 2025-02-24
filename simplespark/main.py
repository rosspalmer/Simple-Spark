import json
import typer

from simplespark.environment.config import SimpleSparkConfig
from simplespark.environment import SimpleSparkEnvironment
from simplespark.setup.build import SetupBuilder
from simplespark.templates import Templates

app = typer.Typer()

@app.command()
def info():
    print(f'TODO')

@app.command()
def setup(config_paths: str, local_host: str = ''):

    print(f'Loading simplespark config file(s): {config_paths}')
    config_files: list[str] = config_paths.split(',')
    config = SimpleSparkConfig.read(*config_files)

    print('Initializing simplespark environment')
    env = SimpleSparkEnvironment(config, local_host)

    print('Setup simplespark environment on hardware')
    SetupBuilder.run(env)


@app.command()
def template(template_type: str, write_path: str):

    print(f'Create template type: {template_type} and write to path: {write_path}')

    template = Templates.generate(template_type)
    template_json = template.get_as_json()

    with open(write_path, 'w') as f:
        json.dump(template_json, f, indent=2)
