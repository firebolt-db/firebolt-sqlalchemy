from firebolt.db import connect


class Connection:
    """
    Compatibility layer for Redash.
    Will be removed once the new version using the SDK is rolled out.
    """

    def __new__(
        cls,
        host: str,
        port: str,
        username: str,
        password: str,
        db_name: str,
        context: str = None,
        header: bool = False,
        ssl_verify_cert: bool = False,
        ssl_client_cert: str = None,
        proxies: str = None,
    ) -> "Connection":
        return connect(
            engine_url=host, database=db_name, username=username, password=password
        )
