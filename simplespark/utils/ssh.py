import os

from paramiko.client import SSHClient


class SSHUtils:

    def __init__(self, host: str, port: int = 22):
        self.host = host
        self.port = port

        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(self.host, port=self.port)
        self.sftp = self.ssh.open_sftp()

    @staticmethod
    def generate_command(root: str, *args, **kwargs) -> str:
        args_string = ' '.join(args)
        kwargs_string = ' '.join([f'--{k} {v}' for k, v in kwargs.items()])
        command = f'{root} {args_string} {kwargs_string}'.strip()
        return command

    def close(self):
        self.ssh.close()
        self.sftp.close()

    def copy(self, local_path: str, remote_path: str):
        self.sftp.put(local_path, remote_path)

    def copy_directory(self, local_path: str, remote_path: str):
        for sub_path in self.sftp.listdir(local_path):
            local_sub_path = os.path.join(local_path, sub_path)
            remote_sub_path = os.path.join(remote_path, sub_path)
            if os.path.isdir(local_sub_path):
                print(f'Found directory: {local_sub_path}')
                self.copy_directory(local_sub_path, remote_sub_path)
            else:
                print(f'Copying {local_sub_path} to {remote_sub_path}')
                self.copy(local_sub_path, remote_sub_path)

    def run(self, command: str):
        stdin, stdout, stderr = self.ssh.exec_command(command)
        return stdin, stdout, stderr
