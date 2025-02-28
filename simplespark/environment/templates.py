
from simplespark.environment.config import *

DEFAULT_VERSIONS = {
    "java": "11.0.21+9",
    "scala": "2.12.18",
    "spark": "3.5.2",
    "delta": "3.2.0",
    "hadoop": "3.3.1",
    "hive": "3.1.2"
}


DEFAULT_JDBC = {
    "postgres": ["org.postgresql", "postgresql", "42.7.4", "org.postgresql.Driver"]
}


class Templates:

    @staticmethod
    def generate(template_type: str, **kwargs) -> SimpleSparkConfig:

        if 'name' not in kwargs:
            kwargs['name'] = '<SIMPLE-SPARK-ENV-NAME>'

        match template_type:

            case "local":
                return Templates.generate_local(**kwargs)

            case "standalone":
                return Templates._generate_standalone(kwargs)

            case _:
                raise Exception(f'Unknown template type: {template_type}')

    @staticmethod
    def generate_local(name: str, with_delta: bool = False) -> SimpleSparkConfig:

        packages = DEFAULT_VERSIONS.copy()
        del packages['hadoop']

        if not with_delta:
            del packages['delta']

        driver = DriverConfig('local')

        config = SimpleSparkConfig(
            name=name,
            setup_type='local',
            packages=packages,
            driver=driver
        )

        return config

    @staticmethod
    def _generate_standalone(kwargs: dict[str, Any]) -> SimpleSparkConfig:

        name = kwargs['name']
        simple_home = kwargs['simple_home']
        profile_path = kwargs['profile_path']

        packages = DEFAULT_VERSIONS.copy()
        del packages['hadoop']

        env = SimpleSparkConfig(name, 'standalone', packages, **kwargs)

        driver = kwargs['driver']

        return env

    @staticmethod
    def _update_kwargs_metastore(kwargs: dict[str, Any]) -> SimpleSparkConfig:

        name = kwargs['name']
        with_delta = kwargs.get('with_delta', False)

        packages = DEFAULT_VERSIONS.copy()
        del packages['hadoop']
        if not with_delta:
            del packages['delta']

        driver = kwargs['driver']

        env = SimpleSparkConfig(name, packages, driver)

        return env


