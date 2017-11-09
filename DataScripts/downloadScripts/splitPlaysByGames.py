import sys
import os

folder = sys.argv[1]
files = os.listdir(folder)
groups = ["first", "second", "third"]
counter = -1
divider = len(files) / 3 + 1
for file in files:
	if os.path.isdir(folder + "/" + file):
		continue
	counter += 1
	with open(folder + "/" + file) as f:
		lines = f.readlines()
		lines = [x.strip() for x in lines]
		folderName = groups[counter / divider]
		if not os.path.exists(folder + "/" + folderName):
			os.makedirs(folder + "/" + folderName)
		count = 0
		for line in lines:
			if line.split(",")[0] == 'id':
				count = 1
				filename = line.split(",")[1]
			elif count == 0:
				break
			with open(folder + "/" + folderName + "/" + filename, 'a+') as tempFile:
				tempFile.write(line + "\n");