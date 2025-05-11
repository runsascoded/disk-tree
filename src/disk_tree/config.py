from os import environ as env, makedirs, walk
from os.path import join, exists

DISK_TREE_ROOT_VAR = 'DISK_TREE_ROOT'
HOME = env['HOME']
CONFIG_DIR = join(HOME, '.config')
ROOT_DIR = join(CONFIG_DIR, 'disk-tree')

if not exists(ROOT_DIR):
    makedirs(ROOT_DIR)

SCANS_DIR = join(ROOT_DIR, 'scans')
SQLITE_PATH = join(ROOT_DIR, 'disk-tree.db')
