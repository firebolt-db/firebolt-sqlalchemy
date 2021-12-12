# from firebolt.db import connect
# from firebolt.async_db import connect
# from firebolt.async_db import connect as async_connect
# import asyncio
# def connect(*args, **kwargs):
#     print("Running connect!")
#     res = get_event_loop().run_until_complete(async_connect(*args, **kwargs))
#     #asyncio.run(async_connect(*args, **kwargs))
#     print("Finished connect")
#     return res

__all__ = [
    "connect",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
]


apilevel = "2.0"
# Threads may share the module and connections
threadsafety = 1
paramstyle = "pyformat"
