from dataclasses import dataclass
import os
import pathlib
import shutil
import subprocess
import zipfile

from simplespark.environment.config import SimpleSparkConfig
from simplespark.utils.ssh import SSHUtils


@dataclass
class CommandReturn:
    command: str
    returncode: int
    stdout: str
    stderr: str


class ShellManager:

    def __init__(self, config: SimpleSparkConfig, host: str = '') -> None:
        self.config = config
        self.host = host

        self.ssh = None
        if self.config.mode != "local":
            if host == '':
                self.ssh = SSHUtils(config.driver.host)
            else:
                self.ssh = SSHUtils(host)


    def run_command(self, command: str) -> CommandReturn:

        if self.config.mode == 'local':
            result = subprocess.Popen(command, text=True)
            return_code = result.returncode
            stdout = result.stdout.read()
            stderr = result.stderr.read()
        else:
            stdin, stdout, stderr = self.ssh.run(command)
            return_code = 0 # TODO

        return CommandReturn(
            command=command,
            returncode=return_code,
            stdout=stdout,
            stderr=stderr
        )

    def archive_and_copy(self, local_package_directory: str, package_destination: str):

        directory = pathlib.Path(local_package_directory)
        temp_archive = ".temp.zip"

        with zipfile.ZipFile(temp_archive, mode="w") as archive:

            for file_path in directory.rglob("*"):
                archive.write(file_path, arcname = file_path.relative_to(directory))

        if self.config.mode == "local":
            shutil.copy(temp_archive, package_destination)
        else:
            self.ssh.copy(temp_archive, package_destination)

        os.remove(temp_archive)

    def spark_submit_python(self, main_file: str, include_packages: str, application_arguments: str = '') -> CommandReturn:

        spark_submit = f"""spark-submit --master spark://{self.config.driver.host}:7077 \
        --py-files={include_packages} \
        {main_file} \
        {application_arguments}
        """

        print(f"Running Python spark-submit:")
        print(spark_submit)

        return self.run_command(spark_submit)
