#-*- coding: utf-8 -*-

""" EOSS catalog system
 Dataset harvesting package
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"


from itertools import (takewhile, repeat)


def count_lines(filename):
    """
    Count lines in file
    :param filename: existing local file
    :return: line numbers
    """
    f = open(filename, 'rb')
    buffer = takewhile(lambda x: x, (f.raw.read(1024 * 1024) for _ in repeat(None)))

    return sum(buf.count(b'\n') for buf in buffer if buf)
