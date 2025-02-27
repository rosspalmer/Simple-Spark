import os

from paramiko.client import SSHClient


class ShellCommand:

    def __init__(self, host: str = None, port: int = None):
        self.host = host
        self.port = port

    @staticmethod
    def generate(root: str, *args, **kwargs) -> str:

        args_string = ' '.join(args)
        kwargs_string = ' '.join([f'--{k} {v}' for k, v in kwargs.items()])

        command = f'{root} {args_string} {kwargs_string}'.strip()

        return command

    def run(self, command: str):

        if self.host is None:
            exit_status = os.popen(command).close()

        else:

            client = SSHClient()
            client.load_system_host_keys()

            if self.port is None:
                client.connect(self.host)
            else:
                client.connect(self.host, self.port)

            stdin, stdout, stderr = client.exec_command(command)

            exit_status = stdout.channel.recv_exit_status()

        return exit_status
