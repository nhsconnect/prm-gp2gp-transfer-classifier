from setuptools import find_packages, setup

main_entrypoint = "prmdata.pipeline.platform_metrics_calculator.main_deprecated:main"
setup(
    name="gp2gp-data-pipeline",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "python-dateutil>=2.8",
        "requests>=2.2",
        "boto3>=1.12",
        "PyArrow>=3.0",
    ],
    entry_points={
        "console_scripts": [
            f"platform-metrics-pipeline={main_entrypoint}",
        ]
    },
)
