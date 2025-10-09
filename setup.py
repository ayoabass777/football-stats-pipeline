from setuptools import setup, find_packages

setup(
    name="football_stats_etl",
    version="0.1.0",
    description="ETL pipeline for football statistics data",
    author="Ayomide Abass",
    packages=find_packages(include=["etl", "etl.*"]),
    install_requires=[
        "psycopg2-binary",
        "PyYAML",
        "requests"
    ],
    python_requires=">=3.8",
)