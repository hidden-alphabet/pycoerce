# pylint: disable=dangerous-default-value, too-few-public-methods

'''
Description
-----------
Extract pyarrow data types from Python objects

Example
-------
>>> import pycoerce.pyarrow as py2pa
>>> import pyarrow.parquet as pq
>>> objects = [{'id': 0, 'value': 1}, {'id': 1, 'value': 4}]
>>> table = py2pa.dumps(objects)
>>> file = '/tmp/pycoerce.parquet'
>>> pq.write_to_dataset(table=table, root_path=file, use_dictionary=True, compression='snappy')
>>> pq.read_table(file)
pyarrow.Table
id: int64
value: int64
'''

import collections
import pyarrow as pa

# Fixes: https://issues.apache.org/jira/browse/ARROW-3080?src=confmacro
def order_by_key(obj):
    '''
    Description
    -----------
    Order a python dictionary by the value of it's keys
    '''
    return dict(collections.OrderedDict(sorted(obj.items())))

class PyarrowTypeEncoder:
    '''
    Description
    -----------
    Convert from python types to pyarrow types.

    Example
    -------
    >>> import pycoerce.pyarrow as py2pa
    >>> encoder = py2pa.PyarrowTypeEncoder()
    >>> encoder.encode(1)
    DataType(int64)
    '''

    def __init__(self, hooks={}):
        self.hooks = {
            int: pa.int64,
            str: pa.string,
            bool: pa.bool_,
            bytes: pa.binary,
            float: pa.float64,
            **hooks
        }

    def _pytype_to_pyarrow_type(self, obj):
        pytype = type(obj)

        if pytype is list:
            if obj:
                return pa.list_(self._pytype_to_pyarrow_type(obj[0]))

            raise TypeError("PyArrow's 'list_' type does not support empty lists.")

        if pytype is dict:
            # Pyarrow, currently, does not correctly handle the management of
            # complexly typed parquet structures. In particular, if the line
            # below this comment is removed, line 70 of this file fails in the
            # manner illustrated below:
            #
            # >>> impport pycoerce.pyarrow as py2pa
            # >>> encoder = py2pa.PyarrowTableEncoder()
            # >>> encoder.encode([{ 'foo': { 'bar': {}, 'bac': {}, 'foo': 1 } }])
            # 	Traceback (most recent call last):
            # 	File "pyarrow.py", line 103, in _list_of_dicts_to_pyarrow_table
            #	File "pyarrow/table.pxi", line 1251, in pyarrow.lib.Table.from_batches
            # 	File "pyarrow/error.pxi", line 81, in pyarrow.lib.check_status
            # 	pyarrow.lib.ArrowInvalid: Schema at index 0 was different:
            # 	foo: struct<bar: struct<>, bac: struct<>, foo: int64>
            #	vs
            #	foo: struct<bac: struct<>, bar: struct<>, foo: int64>
            #
            # But, as shown be the following two examples, the above error only arises due to the
            # ordering of the keys to the nested dictionaries:
            #
            # >>> encoder.encode([{ 'foo': { 'bac': {}, 'bar': {}, 'foo': 1 } }])
            # >>> encoder.encode([{ 'foo': { 'bar': {}, 'bac': {} } }])
            #
            # The solution is thus to order those dictionaries, bykey, which is done
            # below:
            obj = order_by_key(obj)

            types = [self._pytype_to_pyarrow_type(v) for v in obj.values()]
            fields = [(k, v) for k, v in zip(obj.keys(), types)]
            return pa.struct(fields)

        return self.hooks[pytype]()

    def encode(self, obj):
        '''
        Description
        -----------
        Translate python objects into PyArrow types

        Example
        -------
        >>> import pycoerce.pyarrow as py2pa
        >>> encoder = py2pa.PyarrowTypeEncoder()
        >>> encoder.encode("hello world")
        DataType(string)

        >>> encoder.encode(1)
        DataType(int64)

        >>> encoder.encode([1,2])
        ListType(list<item: int64>)

        >>> encoder.encode({ "a": 1, "b": True })
        StructType(struct<a: int64, b: bool>)
        '''
        return self._pytype_to_pyarrow_type(obj)

class PyarrowTableEncoder:
    '''
    Description
    -----------
    Construct a pyarrow.Table object from a list of arbitrarily
    complex python dictionaries
    '''

    def __init__(self, hooks={}):
        self.type_encoder = PyarrowTypeEncoder(hooks=hooks)

    def _list_of_dicts_to_pyarrow_table(self, objects):
        if not objects:
            return pa.Table.from_arrays([], [])

        objects = [order_by_key(obj) for obj in objects]

        columns = list(objects[0].keys())
        values = [list(dict.values()) for dict in objects]
        rows = [pa.array(row) for row in zip(*values)]

        fields = [
            pa.field(column, self.type_encoder.encode(values[0][i]))
            for i, column in enumerate(columns)
        ]
        schema = pa.schema(fields)

        batch = pa.RecordBatch.from_arrays(rows, columns)
        table = pa.Table.from_batches([batch], schema)

        return table

    def encode(self, objs):
        '''
        Description
        -----------
        Convert a list of python dictionaries into a pyarrow.Table object

        Example
        -------
        >>> import pycoerce.pyarrow as py2pa
        >>> encoder = py2pa.PyarrowTableEncoder()
        >>> encoder.encode([{ "foo": 1 }])
        pyarrow.Table
        foo: int64
        '''
        return self._list_of_dicts_to_pyarrow_table(objs)

def dumps(objs, hooks={}):
    '''
    Description
    -----------
    Convert a list of python objects into a pyarrow.Table object
    '''
    return PyarrowTableEncoder(hooks).encode(objs)
