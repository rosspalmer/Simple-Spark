
from simplespark.environment.config import *

DEFAULT_PACKAGES = [
    PackageConfig("java", "8u442-b06"),
    PackageConfig("scala", "2.12.18"),
    PackageConfig("spark", "3.5.5"),
    PackageConfig("delta", "3.2.0"),
    PackageConfig("hadoop", "3.3.1"),
    PackageConfig("hive", "3.1.2")
]


DEFAULT_JDBC = {
    "mysql": ["com.mysql", "mysql-connector-j", "9.2.0", "com.mysql.cj.jdbc.Driver"],
    "postgres": ["org.postgresql", "postgresql", "42.7.4", "org.postgresql.Driver"]
}


class Templates:

    @staticmethod
    def generate(template_type: str, **kwargs) -> SimpleSparkConfig:

        if 'name' not in kwargs:
            kwargs['name'] = '<SIMPLE-SPARK-ENV-NAME>'
        if 'simplespark_home' not in kwargs:
            kwargs['simplespark_home'] = '<SIMPLESPARK-HOME-DIRECTORY>'
        if 'bash_profile_file' not in kwargs:
            kwargs['bash_profile_file'] = '<SHELL-BASH-PROFILE-FILE>'

        match template_type:

            case "local":
                return Templates.generate_local(**kwargs)

            case "standalone":
                return Templates.generate_standalone(**kwargs)

            case _:
                raise Exception(f'Unknown template type: {template_type}')

    @staticmethod
    def generate_local(name: str, simplespark_home: str, bash_profile_file: str,
                       with_delta: bool = False) -> SimpleSparkConfig:

        packages = DEFAULT_PACKAGES.copy()
        Templates._drop_package(packages, 'hadoop')

        if not with_delta:
            Templates._drop_package(packages, 'delta')

        driver = DriverConfig('localhost')

        config = SimpleSparkConfig(
            name=name,
            simplespark_home=simplespark_home,
            bash_profile_file=bash_profile_file,
            mode='local',
            packages=packages,
            driver=driver
        )

        return config

    @staticmethod
    def generate_standalone(name: str, simplespark_home: str, bash_profile_file: str,
                            with_delta: bool = False) -> SimpleSparkConfig:

        packages = DEFAULT_PACKAGES.copy()
        Templates._drop_package(packages, 'hadoop')

        if not with_delta:
            Templates._drop_package(packages, 'delta')

        driver = DriverConfig(
            host='<DRIVER-HOST>',
            cores=4,
            memory='<DRIVER-MEMORY-SIZE>'
        )

        workers = [
            WorkerConfig(
                host='<WORKER-A-HOST>',
                cores=4,
                memory='<WORKER-A-MEMORY-SIZE>',
                instances=2
            ),
            WorkerConfig(
                host='<WORKER-B-HOST>',
                cores=4,
                memory='<WORKER-B-MEMORY-SIZE>',
                instances=8
            ),
        ]

        config = SimpleSparkConfig(
            name,
            simplespark_home,
            bash_profile_file,
            'standalone',
            packages,
            driver,
            workers=workers
        )

        return config

    @staticmethod
    def _drop_package(packages: list[PackageConfig], package_name: str):
        for pc in packages.copy():
            if pc.name == package_name:
                packages.remove(pc)
