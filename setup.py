import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="firebolt-sqlalchemy",
    version="0.0.1",
    author="Raghav Sharma",
    author_email="raghavs@sigmoidanalytics.com",
    description="Package for Sqlalchemy adapter for Firebolt-Superset integration",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/raghavSharmaSigmoid/sqlalchemy_adapter",
    project_urls={
        "Bug Tracker": "https://github.com/raghavSharmaSigmoid/sqlalchemy_adapter",
    },
    install_requires=[
        'sqlalchemy>=1.0.0',
        "requests"
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "firebolt = sqlalchemy_adapter.firebolt_dialect:FireboltDialect"
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)