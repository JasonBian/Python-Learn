#!/usr/bin/python

# import os
# import string
# import sys
#
# from avro import schema
# from avro import io
# from avro import datafile
#
# if __name__ == '__main__':
#     if len(sys.argv) != 2:
#         sys.exit('Usage: %s <data_file>' % sys.argv[0])
#     avro_file = sys.argv[1]
#     print avro_file
#     writer = open(avro_file, 'wb')
#     datum_writer = io.DatumWriter()
#     schema_object = schema.parse(open("/Users/bianzexin/PycharmProjects/HelloWorld/exercise/user.avsc", "rb").read())
#     dfw = datafile.DataFileWriter(writer, datum_writer, schema_object)
#     for line in sys.stdin.readlines():
#         (left, right) = string.split(line.strip(), ',')
#         dfw.append({'left':left,'right':right});
#     dfw.close()

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.parse(open("user.avsc", "rb").read())

writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
writer.append({"name": "Alyssa", "favorite_number": 256})
writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
writer.close()

reader = DataFileReader(open("users.avro", "rb"), DatumReader())
for user in reader:
    print user
reader.close()