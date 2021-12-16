from setuptools import find_packages, setup


setup(
    name="hapi2",
    version="0.1",
    author="Roman Kochanov",
    author_email="",
    description="HITRAN Application Programming Interface (HAPI) v2",
    url="https://github.com/hitranonline/hapi2",
    python_requires=">=3.5",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD-2",
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
