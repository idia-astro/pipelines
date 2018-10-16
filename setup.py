from setuptools import setup, find_packages
import numpy
import glob

setup (
        name="processMeerKAT",
        version = 0.1,
        packages=find_packages(),
        include_package_data=True,
        include_dirs=numpy.get_include(),
        scripts=glob.glob('./processMeerKAT/cal_scripts/*.py'),
        #scripts=['./processMeerKAT/get_fields.py'],
        entry_points="""
        [console_scripts]
        processMeerKAT=processMeerKAT.processMeerKAT:main
        """
)
