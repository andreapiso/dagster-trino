from pathlib import Path
from typing import Dict

from setuptools import find_packages, setup


def get_version() -> str:
    version: Dict[str, str] = {}
    with open(Path(__file__).parent / "dagster_trino/version.py", encoding="utf8") as fp:
        exec(fp.read(), version)  # pylint: disable=W0122

    return version["__version__"]


ver = get_version()
# dont pin dev installs to avoid pip dep resolver issues
pin = "" if ver == "1!0+dev" else f"=={ver}"
setup(
    name="dagster-trino",
    version=ver,
    author="Andrea Pisoni",
    author_email="tbd@gmail.com",
    license="Apache-2.0",
    description="Package for Trino-specific Dagster framework op and resource components.",
    url="https://github.com/andreapiso/dagster-trino",
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(), #exclude=["dagster_trino_tests*"]
    include_package_data=True,
    install_requires=[
        "trino",
        f"dagster{pin}",
    ],
    extras_require={
        "pandas": ["pandas"],
    },
    zip_safe=False,
)