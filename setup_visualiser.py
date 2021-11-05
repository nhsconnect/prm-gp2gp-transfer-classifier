from setuptools import setup

setup(
    name="gp2gp-transfer-visualiser",
    packages=['gp2gpvis'],
    entry_points={
        'console_scripts': ['gp2gp-vis=gp2gpvis.visualiser:main'],
    }
)