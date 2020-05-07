# Python Streaming Data Types
## For developers

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

### Building the package locally and deploying it to PyPI
**First update the version number in setup.py.**

Delete any old builds you may have:
```
rm -rf build dist
```

Build it locally:
```
python setup.py sdist bdist_wheel
```

Check dist files:
```
twine check dist/*
```

Push to test.pypi.org for testing:
```
twine upload --repository-url https://test.pypi.org/legacy/ dist/*  
```

After testing installing from test.pypi.org works, push to PyPI:
```
twine upload dist/*
```
