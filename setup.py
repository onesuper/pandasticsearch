from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

extras_require = {
    "pandas": ["pandas"],
}

setup(
    name='pandasticsearch',
    version='0.2.0',
    author='onesuper',
    author_email='onesuperclark@gmail.com',
    packages=['pandasticsearch'],
    url='http://pypi.python.org/pypi/pandasticsearch/',
    license='MIT',
    description='A Pandastic Elasticsearch client for data analyzing.',
    install_requires=required,
    extras_require=extras_require,
    test_suite='nose.collector',
    tests_require=['nose', 'mock'],
)
