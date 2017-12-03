import urllib2
from bs4 import BeautifulSoup

url = 'http://datachart.500.com/dlt/history/newinc/history.php?limit=20000&sort=0'
req = urllib2.Request(url)
response = urllib2.urlopen(req)
html = response.read()
soup = BeautifulSoup(html)
rows = soup.find('table').find("tbody").find_all("tr")
for row in rows:
    cells = row.find_all("td")
    num = cells[0].get_text()
    front1 = cells[1].get_text()
    front2 = cells[2].get_text()
    front3 = cells[3].get_text()
    front4 = cells[4].get_text()
    front5 = cells[5].get_text()
    behind1 = cells[6].get_text()
    behind2 = cells[7].get_text()
    print num, front1, front2, front3, front4, front5, behind1, behind2


