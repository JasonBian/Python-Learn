dict2 = {'name': 'earth', 'port': 80}
for key in dict2:
    print 'key=%s, value=%s' % (key, dict2[key])
print dict2['name']
print 'serve' in dict2
print dict2.pop('name')
for key in dict2:
    print 'key=%s, value=%s' % (key, dict2[key])
print hash(dict2['port'])


