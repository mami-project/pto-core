from setuptools import setup, find_packages


with open('README.md') as f:
    long_description = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='ptocore',
    version='0.0.1',
    description='Core functionality for the MAMI Path Transparency Observatory.',
    long_description=long_description,
    url='https://github.com/mami-project/pto-core',

    author='Elio Gubser',
    author_email='elio.gubser@alumni.ethz.ch',

    license='',

    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Build Tools',

        'Programming Language :: Python :: 3.5',
    ],

    keywords='',

    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),

    install_requires=['python-dateutil', 'pymongo', 'flask', 'jsonschema'],

    entry_points={
    },
)