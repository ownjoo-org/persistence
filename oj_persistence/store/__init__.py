from oj_persistence.store.abstract_file import AbstractFileStore
from oj_persistence.store.async_base import AsyncAbstractStore
from oj_persistence.store.async_in_memory import AsyncInMemoryStore
from oj_persistence.store.async_ndjson_file import AsyncNdjsonFileStore
from oj_persistence.store.async_sqlite import AsyncSqliteStore
from oj_persistence.store.async_versioned import AsyncVersionedStore
from oj_persistence.store.base import AbstractStore
from oj_persistence.store.csv_file import CsvFileStore
from oj_persistence.store.flat_file import FlatFileStore
from oj_persistence.store.in_memory import InMemoryStore
from oj_persistence.store.ndjson_file import NdjsonFileStore
from oj_persistence.store.sqlite import SqliteStore
from oj_persistence.store.versioned import VersionedStore

__all__ = [
    'AbstractFileStore',
    'AbstractStore',
    'AsyncAbstractStore',
    'AsyncInMemoryStore',
    'AsyncNdjsonFileStore',
    'AsyncSqliteStore',
    'AsyncVersionedStore',
    'CsvFileStore',
    'FlatFileStore',
    'InMemoryStore',
    'NdjsonFileStore',
    'SqliteStore',
    'VersionedStore',
]

try:
    from oj_persistence.store.ijson_file import IjsonFileStore
    __all__.append('IjsonFileStore')
except ImportError:
    pass

try:
    from oj_persistence.store.tinydb_store import TinyDbStore
    __all__.append('TinyDbStore')
except ImportError:
    pass

try:
    from oj_persistence.store.sqlalchemy_store import SqlAlchemyStore
    from oj_persistence.store.async_sqlalchemy_store import AsyncSqlAlchemyStore
    __all__ += ['AsyncSqlAlchemyStore', 'SqlAlchemyStore']
except ImportError:
    pass

try:
    from oj_persistence.store.redis_store import RedisStore
    from oj_persistence.store.async_redis_store import AsyncRedisStore
    __all__ += ['AsyncRedisStore', 'RedisStore']
except ImportError:
    pass
