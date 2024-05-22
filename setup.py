from setuptools import find_packages, setup

setup(
    name="hitran-api2",
    version="0.1",
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
