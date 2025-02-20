import json
import sys
import typer

from simplespark.config import SimpleSparkConfig
from simplespark.environment import SimpleSparkEnvironment
from simplespark.setup.build import SetupBuilder
from simplespark.templates import Templates

app = typer.Typer()

@app.command()
def setup(config_paths: str, local_host: str = ''):

    print(f'Loading simplespark config file(s): {config_paths}')
    config_files: list[str] = sys.argv[1].split(',')
    config = SimpleSparkConfig.read(*config_files)

    print('Initializing simplespark environment')
    env = SimpleSparkEnvironment(config, local_host)

    print('Setup simplespark environment on hardware')
    SetupBuilder.run(env)


@app.command()
def info():
    print(f'TODO')


@app.command()
def template(template_type: str, write_path: str, **kwargs):

    template = Templates.generate(template_type, **kwargs)
    template_json = template.get_as_json()

    with open(write_path, 'w') as f:
        json.dump(template_json, f, indent=2)
