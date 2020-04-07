from setuptools import setup, find_packages


setup(
    name="streaming_data_types",
    version="0.4.0",
    description="Python utilities for handling ESS streamed data",
    long_description="Python utilities for serialising and deserialising data via FlatBuffers for the European Spallation Source ERIC",
    author="ScreamingUdder",
    author_email="NoAddress@Nowhere.com",
    url="https://github.com/ess-dmsc/python-streaming-data-types",
    license="BSD 2-Clause License",
    packages=find_packages(exclude="tests"),
    install_requires=["flatbuffers", "numpy"],
    extras_requires={"dev": ["flake8", "pre-commit", "pytest", "tox"]},
)
