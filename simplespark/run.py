
import sys
from typing import List

from simplespark.environment.config import SimpleSparkConfig
from simplespark.environment import SimpleSparkEnvironment
from simplespark.setup.build import SetupBuilder


if __name__ == "__main__":

    config_files: List[str] = sys.argv[1].split(',')
    if len(sys.argv) > 2:
        local_host = sys.argv[2]
    else:
        local_host = ''

    config = SimpleSparkConfig.read(*config_files)
    env = SimpleSparkEnvironment(config, local_host)

    SetupBuilder.run(env)

