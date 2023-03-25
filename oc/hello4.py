#!/usr/bin/env python3

import logging

VERBOSE_FMT = ('%(asctime)s %(levelname)s %(name)s %(module)s %(process)d %(thread)d '
                   '%(filename)s_%(lineno)s_%(funcName)s  %(message)s')
logging.basicConfig(
                    filename='/aac/python/logs/hello4.log',
                    format=VERBOSE_FMT,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def parse_argument():
    import argparse, pathlib
    parser = argparse.ArgumentParser()
    parser.add_argument("-username", type=str, help="name to say hello", default="joe")
    parser.add_argument("-procdate", type=str, help="processing date")
    args = parser.parse_args()
    username = str(args.username)
    procdate = str(args.procdate)
    return username, procdate

username, procdate = parse_argument()

if __name__ == "__main__":
    logging.info('Hello Python: {}, processing date: {}'.format(username,procdate))
