from setuptools import find_packages, setup

setup(
    name="gp2gp-transfer-classifier",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "python-dateutil>=2.8",
        "boto3>=1.18",
        "PyArrow>=5.0",
        "urllib3==1.26.18",
    ],
)
