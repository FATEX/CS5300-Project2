import os
import string
import pprint as pp

def get_write_EmptyNodes():
	print "Entering get_write_EmptyNodes Function"
	emptyNodes = []
	f = open('NodesWithNoEdges.txt', 'r')
	final = open('PreprocessFinalFile.txt','a')

	staticPR = 1.0/float(685230.0)

	for line in iter(f):
		temp = line.rstrip()

		stringBuilder = temp + " " + str(staticPR) + " " + "0"

		final.write(stringBuilder)
		final.write("\n")

	f.close()
	final.close()


get_write_EmptyNodes()