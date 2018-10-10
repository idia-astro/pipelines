from setuptools import setup, find_packages
import numpy

setup (
        name="processMeerKAT",
        version = 0.1,
        packages=find_packages(),
        include_package_data=True,
        include_dirs=numpy.get_include(),
        entry_points="""
        [console_scripts]
        processmeer=processMeerKAT:main
        """,
)
