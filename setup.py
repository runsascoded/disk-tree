from os.path import dirname, join
from setuptools import setup, find_packages

version = '0.0.2'

dir = dirname(__file__)

with open(join(dir, 'requirements.txt'), 'r') as f:
    install_requires = [ line.rstrip('\n') for line in f.readlines() ]

setup(
    name='disk-tree',
    version=version,
    description='Disk/Cloud space usage analyzer',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    author='Ryan Williams',
    author_email='ryan@runsascoded.com',
    license='Apache License 2.0',
    packages=find_packages(),
    entry_points='''
        [console_scripts]
        disk-tree=disk_tree.main:cli
    ''',
    install_requires = install_requires,
)
