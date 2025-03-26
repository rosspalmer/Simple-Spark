import os

from paramiko.client import SSHClient


class SSHUtils:

    def __init__(self, host: str, port: int = 22, username: str = None):
        self.host = host
        self.port = port
        self.username = username

        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(self.host, port=self.port, username=self.username)
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

    def create_directory(self, path: str):
        try:
            self.sftp.mkdir(path)
        # TODO add better exception handling
        except:
            pass

    def copy_directory(self, local_path: str, remote_path: str):

        if not self.exists(remote_path):
            self.sftp.mkdir(remote_path)

        for sub_path in os.listdir(local_path):
            local_sub_path = os.path.join(local_path, sub_path)
            remote_sub_path = os.path.join(remote_path, sub_path)
            if os.path.isdir(local_sub_path):
                print(f'Found directory: {local_sub_path}')
                self.copy_directory(local_sub_path, remote_sub_path)
            else:
                print(f'Copying {local_sub_path} to {remote_sub_path}')
                self.copy(local_sub_path, remote_sub_path)

    def exists(self, remote_path: str) -> bool:

        path_parts = remote_path.split('/')
        last_part = path_parts[-1]
        root_folder = "/".join(remote_path.split('/')[:-1])
        if remote_path.startswith('/'):
            root_folder = "/" + root_folder

        root_folder_contents = self.sftp.listdir(root_folder)

        return last_part in root_folder_contents

    def run(self, command: str):
        stdin, stdout, stderr = self.ssh.exec_command(command)
        return stdin, stdout, stderr
