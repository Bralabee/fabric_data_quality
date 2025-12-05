"""
Setup script for Fabric Data Quality Framework
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="fabric-data-quality",
    version="1.0.0",
    author="HS2 Data Engineering Team",
    description="Reusable data quality framework for MS Fabric using Great Expectations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Quality Assurance",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "great-expectations>=0.18.0",
        "pyyaml>=6.0",
        "pandas>=1.5.0",
        "openpyxl>=3.1.0",  # For Excel support
        "pyarrow>=12.0.0",  # For Parquet support
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
        ],
    },
)
