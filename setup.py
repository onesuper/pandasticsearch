from setuptools import setup

install_requires = [
    "six >= 1.9.0",
]

extras_require = {
    "official": ["elasticsearch"],
}

setup(
    name='pandasticsearch',
    version='0.0.3',
    author='onesuper',
    author_email='onesuperclark@gmail.com',
    packages=['pandasticsearch'],
    url='http://pypi.python.org/pypi/pandasticsearch/',
    license='MIT',
    description='A Pandastic Elasticsearch client for data analyzing.',
    install_requires=install_requires,
    extras_require=extras_require,
    test_suite='nose.collector',
    tests_require=['pandas', 'nose', 'mock'],
)