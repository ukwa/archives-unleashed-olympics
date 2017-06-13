import sys

print('[')
with open(sys.argv[1]) as f:
  for line in f.readlines():
    print("%s," % line.strip())
print(']')
