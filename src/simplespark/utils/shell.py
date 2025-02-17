import os


class ShellCommand:

    def __init__(self, root: str, *args, **kwargs):
        self.root = root
        self.args = args
        self.kwargs = kwargs

        self.command = self.generate_command_string(root, *args, **kwargs)

    @staticmethod
    def generate_command_string(root: str, *args, **kwargs) -> str:

        args_string = ' '.join(args)
        kwargs_string = ' '.join([f'--{k} {v}' for k, v in kwargs.items()])

        command = f'{root} {args_string} {kwargs_string}'.strip()

        return command

    def run(self) -> int:

        exit_status = os.popen(self.command).close()

        return exit_status
