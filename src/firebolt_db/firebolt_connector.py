from firebolt.db import connect


class Connection:
    """
    Compatibility layer for Redash.
    Will be removed once the new version using the SDK is rolled out.
    """

    def __new__(
        cls,
        host,
        port,
        username,
        password,
        db_name,
        context=None,
        header=False,
        ssl_verify_cert=False,
        ssl_client_cert=None,
        proxies=None,
    ) -> None:
        return connect(
            engine_url=host, database=db_name, username=username, password=password
        )
