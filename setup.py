from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='streaming_data_types',
    version='0.1.0',
    description='Python utilities foe handling ESS streamed data',
    long_description=readme,
    author='ScreamingUdder',
    author_email='NoAddress@Nowhere.com',
    url='https://github.com/ess-dmsc/python-streaming-data-types',
    license=license,
    packages=find_packages(exclude='tests')
)
