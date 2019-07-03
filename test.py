import doctest

from pycoerce import postgresql
from pycoerce import pyarrow

doctest.testmod(postgresql)
doctest.testmod(pyarrow)
