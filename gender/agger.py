import sys
import json
import csv
import re
from urllib.parse import urlparse

# load


totals =  dict()
totals['all'] = dict()
with open(sys.argv[1]) as csvfile:
     gend = csv.reader(csvfile)
     for row in gend:
         date, domain, name, freq, gender = row
         hostname = urlparse(domain).hostname
         if hostname:
             domain = hostname
         sub = totals.get(domain,{})
         count = sub.get(gender, 0)
         count += 1
         sub[gender] = count
         totals[domain] = sub

for key, sub in totals.items():
  line = "%s" % key
  for gender in ['Male', 'Female', 'No Match', 'Unknown']:
    line = "%s,%s" % ( line, sub.get(gender,0))
  print(line)
