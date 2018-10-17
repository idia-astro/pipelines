from setuptools import setup, find_packages
import glob

setup (
        name="processMeerKAT",
        version = 0.1,
        packages=find_packages(),
        include_package_data=True,
        scripts=glob.glob('./processMeerKAT/cal_scripts/*.py'),
        install_requires=['numpy','scipy'],
        entry_points="""
        [console_scripts]
        processMeerKAT=processMeerKAT:main
        """
)
