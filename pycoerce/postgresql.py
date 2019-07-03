# pylint: disable=dangerous-default-value,too-few-public-methods

'''
Description
-----------
Extract PostgreSQL data types from Python objects.

Example
-------
>>> import pycoerce.postgresql as py2pg
>>> columns = py2pg.dumps({ "id": 0, "name": "foo bar" })
>>> table = ", ".join([ "{} {}".format(name, sqltype) for name, sqltype in columns.items()])
>>> stmt = "CREATE TABLE example({});".format(table)
>>> stmt == "CREATE TABLE example(id integer, name text);"
True
'''

from collections.abc import Iterable

# By default, encountering these types will cause the program to
# throw an exception as most SQL implementations don't allow for
# columns with nested data types. By implementing your own decoder,
# you can override this behavior.
NESTED = [range, iter, list, dict, tuple, set, frozenset]

# These python types will cause an exception to be thrown if they're
# encountered without a decoder being given to handle them. This is the
# case for any python type for which there is no obvious default into
# corresponding type in SQL.
UNSUPPORTED = [complex]

class PostgresTypeEncoder:
    '''
    Description
    -----------
    PostgresTypeEncoder encodes Python objects into the corresponding
    types in SQL with the aim of allowing for dynamic SQL queries that
    require type information.
    '''

    def __init__(self, hooks={}):
        # https://www.postgresql.org/docs/8.4/datatype.html
        if not isinstance(hooks, dict):
            raise TypeError(
                '''
                The 'hooks' argument must be a dictionary from classes to, either
                strings or callables.
                '''
            )

        self.hooks = {
            int: 'integer',
            bool: 'boolean',
            bytes: 'bytea',
            bytearray: 'bytea',
            memoryview: 'bytea',
            float: 'real',
            str: 'text',
            **hooks,
        }

    def _resolve_hook(self, pytype):
        resolver = self.hooks[pytype]

        if isinstance(resolver, str):
            return resolver

        if callable(resolver):
            return resolver(pytype)

        raise TypeError(
            '''
            Hooks must be either strings or callable. The hook given for
            python objects of type '{}' is neither.
            '''.format(pytype)
        )

    def _validate_object_types(self, obj):
        types = [type(item) for item in obj]

        for pytype in types:
            if pytype not in self.hooks:
                raise TypeError('SQL does not have a type corresponding to {}.'.format(pytype))

            if pytype in NESTED:
                raise TypeError(
                    '''
                    The type '{}' is nested. Nested types aren't valid in PostgreSQL.
                    You should either flatten the corresponding object of that type
                    into it's container object, or write a decoder to do so dynamically.
                    '''.format(pytype)
                )

            if pytype in UNSUPPORTED:
                raise TypeError(
                    '''
                    The object of type '{}' has no default corresponding type within PostgerSQL.
                    You can resolve this by either removing the object, aliasing it to an
                    PostgreSQL type, or writing a decoder.
                    '''.format(pytype)
                )

        return types

    def _iter_to_sql_types(self, obj):
        types = self._validate_object_types(obj)
        return [self._resolve_hook(t) for t in types]

    def _dict_to_sql_columns(self, obj):
        types = self._validate_object_types(obj.values())
        return {k: self._resolve_hook(t) for k, t in zip(obj.keys(), types)}

    def encode(self, obj):
        '''
        Description
        -----------
        Extracts the SQL datatypes from a python object.

        Example
        -------
        >>> import pycoerce.postgresql as py2pg
        >>> encoder = py2pg.PostgresTypeEncoder()
        >>> encoder.encode(1)
        'integer'
        >>> encoder.encode({ "is_admin": True })
        {'is_admin': 'boolean'}
        '''
        obj_t = type(obj)

        if obj_t in self.hooks:
            return self._resolve_hook(obj_t)

        if obj_t is dict:
            return self._dict_to_sql_columns(obj)

        if isinstance(obj, Iterable):
            return self._iter_to_sql_types(obj)

        raise TypeError(
            '''
            PostgresTypeEncoder cannot encode objects of type '{}' to an SQL
            equivalent
            '''
        )

def dumps(obj, hooks={}):
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
    >>> import pycoerce.postgresql as py2pg
    >>> from datetime import datetime
    >>> example1 = { "foo": "bar", "baz": 5, "buz": datetime.now() }
    >>> py2pg.dumps(example1, hooks={ datetime: 'datetime' })
    {'foo': 'text', 'baz': 'integer', 'buz': 'datetime'}

    >>> example2 = [ 1, "test", True ]
    >>> py2pg.dumps(example2, hooks={ str: 'varchar' })
    ['integer', 'varchar', 'boolean']

    >>> py2pg.dumps("test")
    'text'
    '''

    return PostgresTypeEncoder(hooks).encode(obj)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
