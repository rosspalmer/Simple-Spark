from typing import Any, Callable, Dict

from simplespark.config import SimpleSparkConfig, ResourceConfig


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
    def generate(template_name: str, with_delta: bool, with_hadoop: bool = False, **kwargs):

        name = kwargs['name']
        simple_home = kwargs['simple_home']
        profile_path = kwargs['profile_path']

        packages = DEFAULT_VERSIONS.copy()
        if not with_delta:
            del packages['delta']
        if not with_hadoop:
            del packages['hadoop']

        driver = kwargs['driver']

        env = SimpleSparkConfig(
            name, simple_home, profile_path, packages, driver
        )

        match template_name:

            case "standalone":
                return Templates._generate_standalone(kwargs)

            case "hive_metastore":
                metastore_db = kwargs['metastore_db']
                with_delta = kwargs['with_delta']
                return Templates._generate_hive(metastore_db, with_delta)

    def _update_kwargs(self, template_name: str, kwargs: dict[str, Any]) -> dict[str, Any]:
        match template_name:
            case "standalone":
                return kwargs
            case "hive_metastore":
                return Templates._generate_hive_metastore(metastore_db, with_delta)


    @staticmethod
    def _generate_standalone(kwargs: dict[str, Any]) -> SimpleSparkConfig:

        name = kwargs['name']
        simple_home = kwargs['simple_home']
        profile_path = kwargs['profile_path']

        packages = DEFAULT_VERSIONS.copy()
        del packages['hadoop']

        env = SimpleSparkConfig(
            name, simple_home, profile_path, packages,
            **kwargs
        )

        driver = kwargs['driver']

        return env

    @staticmethod
    def _update_kwargs_metastore(kwargs: dict[str, Any]) -> SimpleSparkConfig:

        name = kwargs['name']
        simple_home = kwargs['simple_home']
        profile_path = kwargs['profile_path']

        with_delta = kwargs.get('with_delta', False)

        packages = DEFAULT_VERSIONS.copy()
        del packages['hadoop']
        if not with_delta:
            del packages['delta']

        driver = kwargs['driver']

        env = SimpleSparkConfig(
            name, simple_home, profile_path, packages, driver
        )

        return env
