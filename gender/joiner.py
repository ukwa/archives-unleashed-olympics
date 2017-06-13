import sys
import json
import csv
import re

# load


genders = dict()
with open('usprocessed.csv') as csvfile:
     gend = csv.reader(csvfile)
     for row in gend:
         name,years_appearing,count_male,count_female,prob_gender,obs_male,est_male,upper,lower = row
         genders[name.lower()] = prob_gender

exs = []
with open(sys.argv[1]) as f:
  for line in f.readlines():
    ex = json.loads(line)
    exs.append(ex)
    for nere in ex['ner']:
      if nere['nerType'] == 'PERSON':
        for ent in nere['entities']:
          gen  = "No Match"
          for named, gender in genders.items():
            if named in ent['entity'].lower():
              regexd = r'\b%s\b' % named
              if re.match(regexd, ent['entity'], re.IGNORECASE):
                gen = gender
          print('%s,%s,%s,%s,%s' % (ex['date'], ex['domain'], ent['entity'], ent['freq'], gen))
