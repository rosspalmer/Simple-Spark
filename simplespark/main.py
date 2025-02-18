import sys
import typer

from simplespark.config import SimpleSparkConfig
from simplespark.environment import SimpleSparkEnvironment
from simplespark.setup.build import SetupTaskBuilder

app = typer.Typer()

@app.command()
def setup(config_paths: str, local_host: str = ''):

    print(f'Loading simplespark config file(s): {config_paths}')
    config_files: list[str] = sys.argv[1].split(',')
    config = SimpleSparkConfig.read(*config_files)

    print('Initializing simplespark environment')
    env = SimpleSparkEnvironment(config, local_host)

    print('Setup simplespark environment on hardware')
    SetupTaskBuilder.run(env)


@app.command()
def info(blah: str):
    print(f'TODO')


if __name__ == '__main__':
    app()
