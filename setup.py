from setuptools import setup

setup(
    name='pypipe',
    version='0.1',
    description='Automated evaluation framework.',
    author='Martin Zoula',
    author_email='zoulamar@fel.cvut.cz',
    license='MIT',
    packages=['pypipe', 'pypipe.modules', 'pypipe.datatypes'],
    scripts=['bin/pypipe'],
    zip_safe=False,
    install_requires=[
       'colored',
       'pyyaml',
       'numpy',
    ]
)

