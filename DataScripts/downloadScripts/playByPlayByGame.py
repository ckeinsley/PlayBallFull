import sys
import os

folder = sys.argv[1]
files = os.listdir(folder)

for file in files:
	os.system("pig -D tez.counters.max=1000 -x tez_local -param input='{}' -param gameId='{}' -param year='{}' -f downloadScripts/storePlayByPlayGame.pig".format(folder + "/" + file, file, sys.argv[2]))
