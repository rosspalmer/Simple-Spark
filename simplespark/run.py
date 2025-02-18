
import sys
from typing import List

from simspark.config import SimpleSparkConfig
from simspark.environment import SimpleSparkEnvironment
from simspark.setup.build import SetupTaskBuilder


if __name__ == "__main__":

    config_files: List[str] = sys.argv[1].split(',')
    if len(sys.argv) > 2:
        local_host = sys.argv[2]
    else:
        local_host = ''

    config = SimpleSparkConfig.read(*config_files)
    env = SimpleSparkEnvironment(config, local_host)

    SetupTaskBuilder.run(env)

