files = os.popen('file *').readlines()

zips = []
problems = []
for f in files:
    if f.endswith('(MS-DOS, OS/2, NT)\n') == True:
        zips.append(f)
    elif f.endswith('data\n') == True:
        problems.append(f)

zipFiles = list(map(lambda x: x.replace(': gzip compressed data, from FAT filesystem (MS-DOS, OS/2, NT)', ''), zips))
problemFiles = list(map(lambda x: x.replace(': data', ''), problems))

z = open('zips', 'a+')
for name in zipFiles:
    z.write(name)

p = open('problems', 'a+')
for name in problemFiles:
    p.write(name)
