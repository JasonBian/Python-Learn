import re
import json

count = 0

with open('/Users/bianzexin/Downloads/ml-10M100K/movies.dat', 'rb') as csv_file:
    content = csv_file.readlines()
    for line in content:
        fixed = re.sub("::", "\t", line).rstrip().split("\t")
    if len(fixed) == 3:
        title = re.sub(" \(.*\)$", "", re.sub('"','', fixed[1]))
        genre = fixed[2].split('|')
        print '{ "create" : { "_index" : "bigmovie", "_type" : "film", "_id" : "%s" } }' % fixed[0]
        print '{ "id": "%s", "title" : "%s", "year":"%s" , "genre":%s }' % (fixed[0],title, fixed[1][-5:-1], json.dumps(genre))


