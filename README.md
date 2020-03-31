# Python Streaming Data Types
Utilities for working with the FlatBuffers schemas used at the European
Spallation Source ERIC for data transport.

https://github.com/ess-dmsc/streaming-data-types

## FlatBuffer Schemas

|name|description|verifiable|
|----|-----------|----------|
|hs00|Histogram schema|Y|
|ns10|NICOS cache entry schema|Y|
|pl72|Run start|N|
|6s4t|Run stop|N|
|f142|Log Data|Y|

### hs00
Schema for histogram data. It is one of the more complicated to use schemas.
It takes a Python dictionary as its input; this dictionary needs to have correctly
named fields.

The input histogram data for serialisation and the output deserialisation data
have the same dictionary "layout".
Example for a 2-D histogram:
```json
hist = {
    "source": "some_source",
    "timestamp": 123456,
    "current_shape": [2, 5],
    "dim_metadata": [
        {
            "length": 2,
            "unit": "a",
            "label": "x",
            "bin_boundaries": np.array([10, 11, 12]),
        },
        {
            "length": 5,
            "unit": "b",
            "label": "y",
            "bin_boundaries": np.array([0, 1, 2, 3, 4, 5]),
        },
    ],
    "last_metadata_timestamp": 123456,
    "data": np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]),
    "errors": np.array([[5, 4, 3, 2, 1], [10, 9, 8, 7, 6]]),
    "info": "info_string",
}
```
The arrays passed in for `data` and `errors` must be NumPy arrays. `bin_boundaries`
can be passed in as a Python list or a NumPy array, but on deserialisation it will be a
NumPy array.

## For developers

### Building the package
```
python setup.py sdist bdist_wheel
```

### Install the commit hooks (important)
There are commit hooks for Black and Flake8.

The commit hooks are handled using [pre-commit](https://pre-commit.com).

To install the hooks for this project run:
```
pre-commit install
```

To test the hooks run:
```
pre-commit run --all-files
```
This command can also be used to run the hooks manually.

### Tox
Tox allows the unit tests to be run against multiple versions of Python.
See the tox.ini file for which versions are supported.
From the top directory:
```
tox
```

