import json
import os
import socket
import typing as t
from codecs import open
from contextlib import contextmanager
from os.path import abspath, dirname, isfile, join
from pathlib import Path
from random import choice
from string import ascii_lowercase, ascii_uppercase, digits
from time import sleep

import docker
import mysql.connector
import pytest
from _pytest._py.path import LocalPath
from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.legacypath import TempdirFactory
from click.testing import CliRunner
from docker import DockerClient
from docker.errors import NotFound
from docker.models.containers import Container
from faker import Faker
from mysql.connector import MySQLConnection, errorcode
from mysql.connector.connection_cext import CMySQLConnection
from mysql.connector.pooling import PooledMySQLConnection
from requests import HTTPError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy_utils import database_exists, drop_database

from . import database, factories, models


def pytest_addoption(parser: "Parser"):
    parser.addoption(
        "--mysql-user",
        dest="mysql_user",
        default="tester",
        help="MySQL user. Defaults to 'tester'.",
    )

    parser.addoption(
        "--mysql-password",
        dest="mysql_password",
        default="testpass",
        help="MySQL password. Defaults to 'testpass'.",
    )

    parser.addoption(
        "--mysql-database",
        dest="mysql_database",
        default="test_db",
        help="MySQL database name. Defaults to 'test_db'.",
    )

    parser.addoption(
        "--mysql-host",
        dest="mysql_host",
        default="0.0.0.0",
        help="Test against a MySQL server running on this host. Defaults to '0.0.0.0'.",
    )

    parser.addoption(
        "--mysql-port",
        dest="mysql_port",
        type=int,
        default=None,
        help="The TCP port of the MySQL server.",
    )

    parser.addoption(
        "--no-docker",
        dest="use_docker",
        default=True,
        action="store_false",
        help="Do not use a Docker MySQL image to run the tests. "
        "If you decide to use this switch you will have to use a physical MySQL server.",
    )

    parser.addoption(
        "--docker-mysql-image",
        dest="docker_mysql_image",
        default="mysql:latest",
        help="Run the tests against a specific MySQL Docker image. Defaults to mysql:latest. "
        "Check all supported versions here https://hub.docker.com/_/mysql",
    )


@pytest.fixture(scope="session", autouse=True)
def cleanup_hanged_docker_containers() -> None:
    try:
        client: DockerClient = docker.from_env()
        for container in client.containers.list():
            if container.name == "pytest_mysql_to_sqlite3":
                container.kill()
                break
    except Exception:
        pass


def pytest_keyboard_interrupt() -> None:
    try:
        client: DockerClient = docker.from_env()
        for container in client.containers.list():
            if container.name == "pytest_mysql_to_sqlite3":
                container.kill()
                break
    except Exception:
        pass


class Helpers:
    @staticmethod
    @contextmanager
    def not_raises(exception: t.Type[Exception]) -> t.Generator:
        try:
            yield
        except exception:
            raise pytest.fail(f"DID RAISE {exception}")

    @staticmethod
    @contextmanager
    def session_scope(db: database.Database) -> t.Generator:
        """Provide a transactional scope around a series of operations."""
        session: Session = db.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


@pytest.fixture
def helpers() -> t.Type[Helpers]:
    return Helpers


@pytest.fixture()
def sqlite_database(tmpdir: LocalPath) -> t.Union[str, Path, "os.PathLike[t.Any]"]:
    db_name: str = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(32))
    return Path(tmpdir.join(Path(f"{db_name}.sqlite3")))


def is_port_in_use(port: int, host: str = "0.0.0.0") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0


class MySQLCredentials(t.NamedTuple):
    """MySQL credentials."""

    user: str
    password: str
    host: str
    port: int
    database: str


@pytest.fixture(scope="session")
def mysql_credentials(pytestconfig: Config) -> MySQLCredentials:
    db_credentials_file: str = abspath(join(dirname(__file__), "db_credentials.json"))
    if isfile(db_credentials_file):
        with open(db_credentials_file, "r", "utf-8") as fh:
            db_credentials: t.Dict[str, t.Any] = json.load(fh)
            return MySQLCredentials(
                user=db_credentials["mysql_user"],
                password=db_credentials["mysql_password"],
                database=db_credentials["mysql_database"],
                host=db_credentials["mysql_host"],
                port=db_credentials["mysql_port"],
            )

    port: int = pytestconfig.getoption("mysql_port") or 3306
    if pytestconfig.getoption("use_docker"):
        while is_port_in_use(port, pytestconfig.getoption("mysql_host")):
            if port >= 2**16 - 1:
                pytest.fail(f"No ports appear to be available on the host {pytestconfig.getoption('mysql_host')}")
            port += 1

    return MySQLCredentials(
        user=pytestconfig.getoption("mysql_user") or "tester",
        password=pytestconfig.getoption("mysql_password") or "testpass",
        database=pytestconfig.getoption("mysql_database") or "test_db",
        host=pytestconfig.getoption("mysql_host") or "0.0.0.0",
        port=port,
    )


@pytest.fixture(scope="session")
def mysql_instance(mysql_credentials: MySQLCredentials, pytestconfig: Config) -> t.Iterator[MySQLConnection]:
    container: t.Optional[Container] = None
    mysql_connection: t.Optional[t.Union[PooledMySQLConnection, MySQLConnection, CMySQLConnection]] = None
    mysql_available: bool = False
    mysql_connection_retries: int = 15  # failsafe

    db_credentials_file = abspath(join(dirname(__file__), "db_credentials.json"))
    if isfile(db_credentials_file):
        use_docker = False
    else:
        use_docker = pytestconfig.getoption("use_docker")

    if use_docker:
        """Connecting to a MySQL server within a Docker container is quite tricky :P
        Read more on the issue here https://hub.docker.com/_/mysql#no-connections-until-mysql-init-completes
        """
        try:
            client = docker.from_env()
        except Exception as err:
            pytest.fail(str(err))

        docker_mysql_image = pytestconfig.getoption("docker_mysql_image") or "mysql:latest"

        if not any(docker_mysql_image in image.tags for image in client.images.list()):
            print(f"Attempting to download Docker image {docker_mysql_image}'")
            try:
                client.images.pull(docker_mysql_image)
            except (HTTPError, NotFound) as err:
                pytest.fail(str(err))

        container = client.containers.run(
            image=docker_mysql_image,
            name="pytest_mysql_to_sqlite3",
            ports={"3306/tcp": (mysql_credentials.host, f"{mysql_credentials.port}/tcp")},
            environment={
                "MYSQL_RANDOM_ROOT_PASSWORD": "yes",
                "MYSQL_USER": mysql_credentials.user,
                "MYSQL_PASSWORD": mysql_credentials.password,
                "MYSQL_DATABASE": mysql_credentials.database,
            },
            command=[
                "--character-set-server=utf8mb4",
                "--collation-server=utf8mb4_unicode_ci",
            ],
            detach=True,
            auto_remove=True,
        )

    while not mysql_available and mysql_connection_retries > 0:
        try:
            mysql_connection = mysql.connector.connect(
                user=mysql_credentials.user,
                password=mysql_credentials.password,
                host=mysql_credentials.host,
                port=mysql_credentials.port,
                charset="utf8mb4",
                collation="utf8mb4_unicode_ci",
            )
        except mysql.connector.Error as err:
            if err.errno == errorcode.CR_SERVER_LOST:
                # sleep for two seconds and retry the connection
                sleep(2)
            else:
                raise
        finally:
            mysql_connection_retries -= 1
            if mysql_connection and mysql_connection.is_connected():
                mysql_available = True
                mysql_connection.close()
    else:
        if not mysql_available and mysql_connection_retries <= 0:
            raise ConnectionAbortedError("Maximum MySQL connection retries exhausted! Are you sure MySQL is running?")

    yield  # type: ignore[misc]

    if use_docker and container is not None:
        container.kill()


@pytest.fixture(scope="session")
def mysql_database(
    tmpdir_factory: TempdirFactory,
    mysql_instance: MySQLConnection,
    mysql_credentials: MySQLCredentials,
    _session_faker: Faker,
) -> t.Iterator[database.Database]:
    temp_image_dir: LocalPath = tmpdir_factory.mktemp("images")

    db: database.Database = database.Database(
        f"mysql+mysqldb://{mysql_credentials.user}:{mysql_credentials.password}@{mysql_credentials.host}:{mysql_credentials.port}/{mysql_credentials.database}"
    )

    with Helpers.session_scope(db) as session:
        for _ in range(_session_faker.pyint(min_value=12, max_value=24)):
            article: models.Article = factories.ArticleFactory()
            article.authors.append(factories.AuthorFactory())
            article.tags.append(factories.TagFactory())
            article.misc.append(factories.MiscFactory())
            for _ in range(_session_faker.pyint(min_value=1, max_value=4)):
                article.images.append(
                    factories.ImageFactory(
                        path=join(
                            str(temp_image_dir),
                            _session_faker.year(),
                            _session_faker.month(),
                            _session_faker.day_of_month(),
                            _session_faker.file_name(extension="jpg"),
                        )
                    )
                )
            session.add(article)

        for _ in range(_session_faker.pyint(min_value=12, max_value=24)):
            session.add(factories.CrazyNameFactory())
        try:
            session.commit()
        except IntegrityError:
            session.rollback()

    yield db

    if database_exists(db.engine.url):
        drop_database(db.engine.url)


@pytest.fixture()
def cli_runner() -> t.Iterator[CliRunner]:
    yield CliRunner()
