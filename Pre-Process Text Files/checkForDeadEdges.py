import os
import string
import pprint as pp

def checkDeadNodes():
	print "Entering checkDeadNodes Function"
	f = open('PreprocessFinalFile.txt', 'r')
	counter = 0

	for line in iter(f):
		theList = []
		edgesList = []

		theList = line.split(" ")

		#print theList

		if len(theList) != 4:
			print "Error with line length node %s" % str(theList[0])
		else:
			edgesList = theList[3].split(",")
			if len(edgesList) == 0:
				print "Dead Edges For Node %s" % str(theList[0])

		counter += 1

	print counter

	f.close()
	#pp.pprint(edgeNodes)


checkDeadNodes()
print ">>>>DONE!<<<<"