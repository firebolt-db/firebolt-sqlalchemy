import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="firebolt-sqlalchemy",
    version="0.0.5",
    author="Firebolt",
    author_email="pypi@firebolt.io",
    description="Sqlalchemy adapter for Firebolt",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/firebolt-db/firebolt-sqlalchemy",
    download_url="https://github.com/firebolt-db/firebolt-sqlalchemy/archive/refs/tags/0.0.3.tar.gz",
    project_urls={
        "Bug Tracker": "https://github.com/firebolt-db/firebolt-sqlalchemy",
    },
    install_requires=[
        'sqlalchemy>=1.0.0',
        "requests",
        "datetime"
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