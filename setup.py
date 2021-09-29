import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="firebolt-sqlalchemy",
    version="0.0.1",
    author="Raghav Sharma",
    author_email="raghavs@sigmoidanalytics.com",
    description="Sqlalchemy adapter for Firebolt",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/raghavSharmaSigmoid/firebolt-sqlalchemy",
    download_url="https://github.com/raghavSharmaSigmoid/firebolt-sqlalchemy/archive/refs/tags/0.0.1.tar.gz",
    project_urls={
        "Bug Tracker": "https://github.com/raghavSharmaSigmoid/firebolt-sqlalchemy",
    },
    install_requires=[
        'sqlalchemy>=1.0.0',
        "requests",
        "json",
        "itertools",
        "collections",
        "datetime",
        "functools"
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "firebolt = firebolt_db.firebolt_dialect:FireboltDialect"
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