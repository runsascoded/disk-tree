from os import environ as env, makedirs, walk
from os.path import join, exists

DISK_TREE_ROOT_VAR = 'DISK_TREE_ROOT'
HOME = env['HOME']
CONFIG_DIR = join(HOME, '.config')
if exists(CONFIG_DIR):
    ROOT_DIR = join(CONFIG_DIR, 'disk-tree')
else:
    ROOT_DIR = join(HOME, '.disk-tree')

if not exists(ROOT_DIR):
    makedirs(ROOT_DIR)

SQLITE_PATH = join(ROOT_DIR, 'disk-tree.db')
