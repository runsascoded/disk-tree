"""py2app entry point for disk-tree.app — a thin wrapper so the bundle's main
script is unambiguous. Real logic lives in `disk_tree.desktop`."""
from disk_tree.desktop import main

if __name__ == '__main__':
    main()
