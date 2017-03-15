#!/usr/bin/env python

'makeTextFile.py -- create text file'

import os
ls = os.linesep

#get filename

fname='pytest.txt'

while True:

    if os.path.exists(fname):
        print "Error: '%s' already exists" % fname
    else:
        break

#get file content (text) lines
all = []
print "\n Enter lines ('.' by itself to quit).\n"

#loop until user terminates input
while True:
    entry = raw_input('> ')
    if entry == '.':
        break
    else:
        all.append(entry)

# write lines to file with proper line-ending
fobj = open(fname, 'w')
fobj.writelines(['%s%s' % (x, ls) for x in all])
fobj.close()
print 'DONE!'
