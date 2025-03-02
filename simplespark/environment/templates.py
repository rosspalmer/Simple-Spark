
from simplespark.environment.config import *

DEFAULT_PACKAGES = [
    PackageConfig("java", "11.0.21+9"),
    PackageConfig("scala", "2.12.18"),
    PackageConfig("spark", "3.5.2"),
    PackageConfig("delta", "3.2.0"),
    PackageConfig("hadoop", "3.3.1"),
    PackageConfig("hive", "3.1.2")
]


DEFAULT_JDBC = {
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
            setup_type='local',
            packages=packages,
            driver=driver
        )

        return config

    @staticmethod
    def generate_standalone(kwargs: dict[str, Any]) -> SimpleSparkConfig:

        name = kwargs['name']
        simple_home = kwargs['simple_home']
        bash_profile_file = kwargs['bash_profile_file']

        packages = DEFAULT_PACKAGES.copy()
        del packages['hadoop']

        env = SimpleSparkConfig(name, 'standalone', packages, **kwargs)

        driver = kwargs['driver']

        return env

    @staticmethod
    def _drop_package(packages: list[PackageConfig], package_name: str):
        for pc in packages.copy():
            if pc.name == package_name:
                packages.remove(pc)

    @staticmethod
    def _update_kwargs_metastore(kwargs: dict[str, Any]) -> SimpleSparkConfig:

        name = kwargs['name']
        with_delta = kwargs.get('with_delta', False)

        packages = DEFAULT_PACKAGES.copy()
        del packages['hadoop']
        if not with_delta:
            del packages['delta']

        driver = kwargs['driver']

        env = SimpleSparkConfig(name, packages, driver)

        return env
