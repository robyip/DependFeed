import re
str = "create tblRobtest AS rtt"
x = re.findall('.*?(tbl[a-zA-Z0-9]+)\s', str)
if x != 0:
    print(x)
    
str1 = "create tblRobtestRaw AS rtt and another table tblJenChen "
x1 = re.findall('.*?(tbl[a-zA-Z0-9]+Raw)\s', str1)
if x1 != 0:
    print(x1)
    