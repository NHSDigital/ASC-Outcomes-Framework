# See https://nhsd-git.digital.nhs.uk/data-services/analytics-service/iuod/rap-community-of-practice/-/blob/master/python/project-structure-and-packaging.md

from setuptools import find_packages, setup

setup(
    name='my_publication_code',
    packages=find_packages(),
    version='0.1.0',
    description='To create publication ...',
    author='NHS_Digital',
    license='',
    setup_requires=['pytest-runner','flake8'],
    tests_require=['pytest'],
)
