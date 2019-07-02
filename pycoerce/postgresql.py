from collections.abc import Iterable

# By default, encountering these types will cause the program to 
# throw an exception as most SQL implementations don't allow for 
# columns with nested data types. By implementing your own decoder, 
# you can override this behavior.
NESTED = [ range, iter, list, dict, tuple, set, frozenset ]

# These python types will cause an exception to be thrown if they're
# encountered without a decoder being given to handle them. This is the
# case for any python type for which there is no obvious default into
# corresponding type in SQL.
UNSUPPORTED = [ complex ]

class PostgreSQLEncoder:
    '''
    Description
    -----------
    PostgreSQLEncoder encodes Python objects into the corresponding
    types in SQL with the aim of allowing for dynamic SQL queries that
    require type information.
    '''

    def __init__(self, hooks={}):
        # https://www.postgresql.org/docs/8.4/datatype.html 
        if not isinstance(hooks, dict):
            raise TypeError("The 'hooks' argument must be a dictionary from classes to, either strings or callables.")

        self.hooks = {
            None: self._reject_type,
            type(None): self._reject_type, 
            **hooks,
            int: 'integer',
            bool: 'boolean',
            bytes: 'bytea',
            bytearray: 'bytea',
            memoryview: 'bytea',
            float: 'real',
            str: 'text'
        }
    
    def _reject_type(self, t):
        raise TypeError('SQL does not have a type corresponding to {}.'.format(t))

    def _get_object_types(self, obj):
        return [type(item) for item in obj]

    def _resolve_hook(self, pytype):
        resolver = self.hooks[pytype]

        if isinstance(resolver, str):
            return resolver
        elif callable(resolver):
            return resolver(pytype)
        else:
            raise TypeError(
                '''
                Hooks must be either strings or callable. The hook given for
                python objects of type '{}' is neither.
                '''.format(pytype)
            )

    def _validate_object_types(self, obj):
        types = self._get_object_types(obj)

        for t in types:
            if t in self.hooks:
                continue

            if t in NESTED:
                raise TypeError(
                    '''
                    The type '{}' is nested. Nested types aren't valid in PostgreSQL. 
                    You should either flatten the corresponding object of that type
                    into it's container object, or write a decoder to do so dynamically.
                    '''.format(t)
                )

            if t in UNSUPPORTED:
                raise TypeError(
                    '''
                    The object of type '{}' has no default corresponding type within PostgerSQL.
                    You can resolve this by either removing the object, aliasing it to an
                    PostgreSQL type, or writing a decoder.
                    '''.format(t)
                )

        return types 

    def _iter_to_sql_types(self, obj):
        types = self._validate_object_types(obj)
        return [self._resolve_hook(t) for t in types]

    def _dict_to_sql_columns(self, obj):
        types = self._validate_object_types(obj.values())
        return { k: self._resolve_hook(t) for k, t in zip(obj.keys(), types) }

    def encode(self, obj):
        obj_t = type(obj)

        if obj_t in self.hooks:
            return self._resolve_hook(obj_t)
        elif obj_t is dict:
            return self._dict_to_sql_columns(obj)
        elif isinstance(obj, Iterable):
            return self._iter_to_sql_types(obj)
        else:
            raise TypeError(
                '''
                PostgreSQLEncoder cannot encode objects of type '{}' to an SQL
                equivalent 
                '''
            )


def dumps(obj, hooks=None):
    '''
    Description
    -----------
    Converts an arbitrary python object to the corresponding
    type representation in SQL. 

    Usage
    -----
    obj: dict, required
    hooks: dict[str, callable], optional
        A dictionary of decoders mapping a python
        type to a string representation of an SQL type.

    Example
    -------
    >>> from coerceion.to import postgresql
    >>> from datetime import datetime

    >>> example1 = { "foo": "bar", "baz": 5, "buz": datetime.now() }
    >>> postgresql.dumps(example, hooks={ datetime: 'datetime' })
    { "foo": "varchar", "baz": "integer", "buz": "datetime" }

    >>> example2 = [ 1, "test", True ]
    >>> postgresql.dumps(example2, hooks={ str: 'text' })
    ['int', 'text', 'boolean']

    >>> postgresql.dumps("test")
    'varchar'
    '''

    return PostgreSQLEncoder(hooks).encode(obj)
