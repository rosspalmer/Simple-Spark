
from urllib.request import urlretrieve

from simplespark.environment.config import MavenConfig


class MavenDownloader:
    BASE_URL = "https://repo1.maven.org/maven2"

    @staticmethod
    def maven_url(maven_jar: MavenConfig) -> str:
        package_url = f"{maven_jar.group_id.replace('.','/')}/{maven_jar.artifact_id}/{maven_jar.version}"
        return f"{MavenDownloader.BASE_URL}/{package_url}"

    @staticmethod
    def download_jar(maven_jar: MavenConfig, download_folder: str):

        maven_url = MavenDownloader.maven_url(maven_jar)
        jar_filename = f"{maven_jar.artifact_id}-{maven_jar.version}.jar"
        maven_jar_path = f"{maven_url}/{jar_filename}"
        download_path = f"{download_folder}/{jar_filename}"

        print(f'Downloading JAR from {maven_jar_path} to {download_path}')
        urlretrieve(maven_jar_path, download_path)
