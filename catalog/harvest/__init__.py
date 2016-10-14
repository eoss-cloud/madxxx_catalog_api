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


from itertools import (takewhile,repeat)

def count_lines(filename):
    f = open(filename, 'rb')
    bufgen = takewhile(lambda x: x, (f.raw.read(1024*1024) for _ in repeat(None)))
    return sum( buf.count(b'\n') for buf in bufgen if buf )
