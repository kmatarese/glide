#!/usr/bin/env python

from contextlib import contextmanager
import os

from setuptools import find_packages, setup


def find_deploy_scripts(path, include_patterns, exclude_patterns=[]):
    cmd = "FILES=`find %s -path %s" % (path, (" -o -path ").join(include_patterns))
    if exclude_patterns:
        cmd += " | grep -v -E '(%s)'" % ("|").join(exclude_patterns)
    cmd += "`;"
    cmd += " for FILE in $FILES; do if [ `echo $FILE | xargs grep -l '/usr/bin/env python'` ] || [ `echo $FILE | grep -v .py` ]; then echo $FILE; fi; done"
    h = os.popen(cmd)
    out = h.read()
    h.close()
    return out.split()


@contextmanager
def load_file(fname):
    f = open(os.path.join(os.path.dirname(__file__), fname))
    try:
        yield f
    finally:
        f.close()


with load_file("README.md") as f:
    README = f.read()

with load_file("requirements.txt") as f:
    requires = f.read().split("\n")

setup(
    name="glide",
    description="A data processing / ETL pipeline tool",
    long_description=README,
    author="Kurt Matarese",
    maintainer="Kurt Matarese",
    version="0.1.0",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.6",
    scripts=find_deploy_scripts(
        "glide", ["\\*.py", "\\*.sh", "\\*.sql"], ["__init__"]
    ),
    packages=find_packages(),
    include_package_data=True,
    install_requires=requires,
    extras_require={"swifter": ["swifter>=0.289"],
                    "pymysql": ["pymysql"],
                    "dev": ["sphinx", "m2r"],
                    "dask": ["dask>=2.1.0"]},
)
