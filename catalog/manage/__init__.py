# _____________________________________________________________________
# | ................................................................... |
# | ................................................@@@@............... |
# | .@...........................................@@@@@@@@@..@@@@....... |
# | .@@@........................................@@.......@@@@@@@@@@.... |
# | .@.@@@.....................................@.........@.......@@.... |
# | .@...@@@...................................@........@.............. |
# | .@.....@@@.............@@@@@@@@@@@@.......@@.......@............... |
# | .@.......@@@.........@@@.........@@@@.....@........@............... |
# | .@@........@@@.....@@...............@@@...@@.......@............... |
# | .@@...........@@@.@...................@@...@.......@............... |
# | .@@.............@@@....................@@..@@......@@.............. |
# | .@@.............@..@@@..................@@..@@......@@............. |
# | .@@............@......@@@................@@..@@@....@@@............ |
# | .@@...........@..........@@@..............@...@@@@....@@@.......... |
# | .@@...........@.............@@@@..........@@....@@@@...@@@@........ |
# | .@@...........@.................@@@@@......@.......@@@...@@@@...... |
# | .@@@@........@......................@@@@@@@@.........@@.....@@@.... |
# | .@@@@@@......@.............................@..........@@.....@@@... |
# | .@@..@@@@....@.............................@...........@@......@@.. |
# | .@@.....@@@@.@.............................@............@@......@@. |
# | .@@.......@@@@@............................@.............@.......@@ |
# | .@@.........@@@@@..........................@.............@.......@@ |
# | .@............@@@@@.......................@..............@.......@@ |
# | .@............@@..@@@@....................@..............@........@ |
# | .@.............@@...@@@@@................@...............@........@ |
# | .@.............@@@.....@@@@@@...........@@..............@.........@ |
# | .@..............@@@........@@@@@@......@@..............@.........@. |
# | @@@@.............@@@...........@@@@@@@@@...........@@@@.........@.. |
# | ..@@@@@@...........@@@.............@@@@@@@@@@@@@@@@@...........@... |
# | .....@@@@@@@.........@@@@@......@@@@..........................@.... |
# | ..........@@@@@@@......@@@@@@@@@@@..........................@@..... |
# | ...............@@@@@@@@..................................@@@....... |
# | ....................@@@@@@@@@@@......................@@@@@......... |
# | ...........................@@@@@@@@@@@@@@@@@@@@@@@@@@@............. |
# | .....................................@@@@@@@@@@@................... |
# | ................................................................... |
# | ___________________________________________________________________ |
#
# Created by sgebhardt at 11.08.16
# Copyright EOSS GmbH 2016

from abc import ABCMeta, abstractmethod

from utilities import with_metaclass


@with_metaclass(ABCMeta)
class ICatalog(object):
    def __init__(self):
        pass

    @abstractmethod
    def find(self):
        pass

    @abstractmethod
    def register(self):
        pass
