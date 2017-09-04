import itchat

itchat.auto_login(enableCmdQR=2)
friends = itchat.get_friends(update=True)[0:]
male = female = other = 0
for i in friends[1:]:
	sex = i["Sex"]
	if sex == 1:
		male+= 1
	elif sex == 2:
		female += 1
	else:
		other += 1
total = len(friends[1:])
print 'Male Friends:' + male + ',Female Friends:' + female + ', and other:' + other
