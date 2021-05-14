from setuptools import setup, find_packages


INSTALL_REQUIRES = [
    'networkx',
    'numpy',
    'pandas',
]
EXTRAS_REQUIRE = {
    'dev': [
        'pytest',
    ]
}


setup(
    name='dagger',
    version='0.0.1',
    author='Charles Zou',
    requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    python_requires='>=3.9',
)