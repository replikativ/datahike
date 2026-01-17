"""Datahike Python bindings setup."""
from setuptools import setup, find_packages

# Read version from _version.py
version = {}
with open("src/datahike/_version.py") as f:
    exec(f.read(), version)

setup(
    name='datahike',
    version=version['__version__'],
    description="Python bindings for Datahike - a durable Datalog database.",
    long_description=open('README.md').read() if __import__('os').path.exists('README.md') else '',
    long_description_content_type='text/markdown',
    author='Datahike Team',
    author_email='info@replikativ.io',
    url='https://github.com/replikativ/datahike',
    packages=find_packages("src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        'cbor2>=5.4.0',
    ],
    extras_require={
        'dev': [
            'pytest>=7.0',
            'pytest-cov>=4.0',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Eclipse Public License 1.0 (EPL-1.0)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Database',
    ],
)
