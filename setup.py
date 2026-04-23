from pathlib import Path

from setuptools import find_packages, setup

version_ns = {}
version_path = Path(__file__).parent / "hapi2" / "version.py"
exec(version_path.read_text(encoding="utf-8"), version_ns)

setup(
    name="hitran-api2",
    version=version_ns["__version__"],
    author="Roman Kochanov",
    author_email="",
    description="HITRAN Application Programming Interface (HAPI) v2",
    url="https://github.com/hitranonline/hapi2",
    python_requires=">=3.5",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "hitran-api",
        "SQLAlchemy",
        "numpy",
        "numba",
        "tabulate",
        "python-dateutil",
        "pyparsing",
        "scipy",
        "matplotlib",
        "jupyter",
    ],
)
