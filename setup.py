from setuptools import setup, find_packages

setup(
    name="spark-analyzer",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "numpy>=1.21.0",
        "pandas>=1.5.0",
        "plotly>=5.13.0"
    ]
)
