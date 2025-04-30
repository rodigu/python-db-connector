# PyDBCon (the Python DataBase Connector)

simplified appending of dictionary-like values to a SQL database.

PyDBCon is in essence a wrapper for the [`pyodbc`](https://pypi.org/project/pyodbc/) library.

## contents

- [PyDBCon (the Python DataBase Connector)](#pydbcon-the-python-database-connector)
  - [contents](#contents)
  - [installation](#installation)
  - [basic usage](#basic-usage)
  - [type mapper](#type-mapper)
  - [batch inserts](#batch-inserts)

## installation

PyDBCon is hosted as a package on this repository, and can be installed through pip:

```bash
python -m pip install git+https://github.com/rodigu/python-db-connector -U
```

## basic usage

to get started, all you need is a SQL [database connection string](https://learn.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver16), a table, a dictionary-like you want to append to the table, and a dictionary specifying the type mapping:

```py
from pydbcon.connector import DBConnector

connection_string = """
Driver={SQL Server};
Server=SERVER_IP;
Database=DB_NAME;
UID=USER_ID;
PWD=PASSWORD;
Trusted_Connection=no;
"""

dbcon = DBConnector(connection_string=connection_string, table='TABLE_NAME', type_mapper=type_mapper)

dbcon.insert_dict(dict_like)
```

## type mapper

The `DBConnector` class should also be given an instance of the `TypeMapper` class or a dictionary that fits the `TypeMapper` constructor signature.

Example of a `TypeMapper` instance given to the `DBConnector` constructor:

```py
from pydbcon.connector import DBConnector, TypeMapper

type_mapper = TypeMapper(
    direct={'sample_column': 'int', 'another_column': 'varchar(10)'},
    prefix={'pre_': 'varchar(10)'},
    suffix={'_su': 'int'},
    typed={'int64': 'int', 'float64': 'decimal', 'bool': 'bit', 'object': 'varchar(max)'}
)

dbcon = DBConnector(connection_string=connection_string, table='TABLE_NAME', type_mapper=type_mapper)
```

Giving a dictionary as a `type_mapper`:

```py
from pydbcon.connector import DBConnector, TypeMapper

dbcon = DBConnector(
    connection_string=connection_string,
    table='TABLE_NAME',
    type_mapper={
        "direct": {'sample_column': 'int', 'another_column': 'varchar(10)'},
        "prefix": {'pre_': 'varchar(10)'},
        "suffix": {'_su': 'int'},
        "typed": {'int64': 'int', 'float64': 'decimal', 'bool': 'bit', 'object': 'varchar(max)'}
    }
)
```

The `TypeMapper` class helps with the conversion of Python/Pandas data types to SQL types.

The priority chain, in cases when a value shows up more than once: `direct -> prefix -> suffix -> typed`

The keys for a type mapper are:

- `direct`: direct mapping, will map from column name (`dict` key) to SQL type (`dict` value)
- `prefix`: will map from column name that has the given prefix (`dict` key) to SQL type (`dict` value), defaults to an empty dictionary
- `suffix`: will map from column name that has the given suffix (`dict` key) to SQL type (`dict` value), defaults to an empty dictionary
- `typed`: will map from a pandas type (`dict` key) to a SQL type (`dict` value), defaults to an empty dictionary

## batch inserts

as of `v0.7.x`, pydbcon now supports batch inserts of dictionaries with [`append_to_batch`](./src/pydbcon/connector.py#L461) and [`execute_batch`](./src/pydbcon/connector.py#L516)
