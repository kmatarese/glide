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

# Split git requirements to fill in dependency_links
git_requires = [x for x in requires if "git" in x]
non_git_requires = [x for x in requires if "git" not in x]
for repo in git_requires:
    # Append git egg references
    non_git_requires.append(repo.split("egg=")[-1])

extras_require = {
    "swifter": ["swifter~=1.0.6"],
    "rq": ["rq~=1.5.0"],
    "celery": [
        "celery[redis]==4.4.0",
        # Downgrade kombu due to https://github.com/celery/kombu/issues/1063
        "kombu==4.5.0",
    ],
    "dev": [
        "black",
        "fake-useragent~=0.1.11",
        "m2r~=0.2.1",
        "numpydoc~=0.9.2",
        "pre-commit",
        "pylint",
        "pytest~=5.3.2",
        "pytest-redis~=2.0.0",
        "pytest-xprocess~=0.13.1",
        "sphinx~=2.3.1",
        "twine~=3.1.1",
        "wheel",
    ],
    "dask": ["dask[complete]~=2020.12.0", "distributed==2020.12.0"],
}
extras_require["complete"] = sorted(set(sum(extras_require.values(), [])))

exec(open("glide/version.py").read())

setup(
    name="glide",
    description="Easy ETL",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/kmatarese/glide",
    author="Kurt Matarese",
    author_email="none@none.com",
    maintainer="Kurt Matarese",
    version=__version__,
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.6",
    scripts=find_deploy_scripts("glide", ["\\*.py", "\\*.sh", "\\*.sql"], ["__init__"]),
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    install_requires=non_git_requires,
    dependency_links=git_requires,
    extras_require=extras_require,
)
