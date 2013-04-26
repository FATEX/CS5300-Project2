import os
import string
from collections import Counter
import pprint as pp

def main():
	longList = []
	sortedDictionary = {}
	cnt = Counter()
	f = open("Preprocess_76.txt","r")
	degressFile = open("DegressOfNodes.txt", "w") 
	for line in iter(f):
		temp = line.split()
		holder = temp[0]
		longList.append(holder)
	cnt = Counter(longList)
	#counterDict = dict(cnt)
	#counterList = cnt.items()

	#Make Dictionary
	for x in range(0,685230):
		sortedDictionary[str(x)] = cnt[str(x)]
	#pp.pprint(sortedDictionary)

	for x in range(0,685230):
		stringToWrite = "" + str(x) + " " + str(sortedDictionary[str(x)])
		degressFile.write(stringToWrite)
		degressFile.write("\n")

	f.close()
	degressFile.close()
	print "Done!"

	#for (key, value) in sorted(counterDict.iteritems()):
	#	stringBuilder = "" + key + " " + str(value)
	#	print stringBuilder
		#degressFile.write(stringBuilder)

main()