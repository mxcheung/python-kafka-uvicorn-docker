#!/usr/bin/env python3

import logging

VERBOSE_FMT = ('%(asctime)s %(levelname)s %(name)s %(module)s %(process)d %(thread)d '
                   '%(filename)s_%(lineno)s_%(funcName)s  %(message)s')
logging.basicConfig(
                    filename='/aac/python/logs/hello4.log',
                    format=VERBOSE_FMT,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
if __name__ == "__main__":
    logging.info("Hello Python! \n")
