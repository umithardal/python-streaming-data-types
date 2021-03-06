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

### Adding new schemas checklist (important)
* Add unit-tests (see existing tests for an example)
* Update `fbschemas.__init__.py` to include the new serialiser and deserialiser

### Tox
Tox allows the unit tests to be run against multiple versions of Python.
See the tox.ini file for which versions are supported.
From the top directory:
```
tox
```

### Building the package locally and deploying it to PyPI
**First update the version number in setup.py and push the update to the repository.**

#### Requirements
* A [PyPi](https://pypi.org/) account
* A [TestPyPi](https://test.pypi.org/) account (this is separate to the PyPi account)
* Permission to push to the ess-streaming-data-types project on TestPyPi and PyPi
* Installed all requirements in `requirements-dev.txt`

#### Steps

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

The new module can then be installed from test.pypi.org like so:
```
pip uninstall ess_streaming_data_types    # Remove old version if present
pip install -i https://test.pypi.org/simple/ ess-streaming-data-types
```
Unfortunately, flatbuffers is not on test.pypi.org so the following error may occur:
```
ERROR: Could not find a version that satisfies the requirement flatbuffers
```
The workaround is install flatbuffers manually first using `pip install flatbuffers` and then rerun the previous command.

Test the module using the existing test-suite (from project root):
```
rm -rf streaming_data_types    # Rename the local source directory
pytest    # The tests will be run against the pip installed module
git reset --hard origin/master    # Put everything back to before
```

After testing installing from test.pypi.org works, push to PyPI:
```
twine upload dist/*
```
Finally, create a tag on the GitHub repository with the appropriate name, e.g. `v0.7.0`.
