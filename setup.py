from os.path import dirname, join
from setuptools import setup, find_packages

version = '0.0.1'

dir = dirname(__file__)

with open(join(dir, 'requirements.txt'), 'r') as f:
    install_requires = [ line.rstrip('\n') for line in f.readlines() ]

# with open(join(dir, 'requirements-test.txt'), 'r') as f:
#     tests_require = [ line.rstrip('\n') for line in f.readlines() ]

setup(
    name='disk-tree',
    version=version,
    description='Disk/Cloud space usage analyzer',
    author='Ryan Williams',
    author_email='ryan@runsascoded.com',
    license='Apache License 2.0',
    packages=find_packages(),
    #py_modules=['disk_tree', ],
    entry_points='''
        [console_scripts]
        disk-tree=disk_tree.main:cli
    ''',
    install_requires = install_requires,
    # extras_require = { 'test': tests_require },
    # tests_require = tests_require,
)
