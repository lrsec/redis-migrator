# -*- coding: utf-8 -*-

import sys

from redismigrate import migrator


def main():
    if (len(sys.argv)<3):
        print 'Format error. Usage: python redismigrate.py config_file_location migrate/verify'
        return

    config_file = sys.argv[1]
    method_name = sys.argv[2]

    m = migrator.Migrator(config_file)

    try:
        method = getattr(m, method_name)
        method()
    except Exception as e:
        print 'Exception: {}'.format(e)

if __name__ == '__main__':
    main()