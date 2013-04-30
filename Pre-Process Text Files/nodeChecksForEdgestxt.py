import os
import string
import pprint as pp

def checkColumnsedges():
	print "Entering checkColumnsedges Function"
	f = open('edges.txt', 'r')
	leftColumn = []
	rightColumn = []

	for line in iter(f):
		
		temp = []

		x = line.strip()
		x = x.replace("  "," ")
		temp = x.split()

		#print temp

		leftColumn.append(temp[0])
		rightColumn.append(temp[1])

		#print leftColumn

	f.close()
	leftColumn = set(leftColumn)
	lenLeftColumn = len(leftColumn)
	#print lenLeftColumn

	rightColumn = set(rightColumn)
	lenrightColumn = len(rightColumn)

	differenceSet = rightColumn.difference(leftColumn)
	#print lenrightColumn
	print "From Edges.txt the left column had %s unique nodes and the right column had %s." % (lenLeftColumn, lenrightColumn)
	print differenceSet

def checkColumnsPreProcess():
	print "Entering checkColumnsPreProcess Function"
	f = open('Preprocess_76.txt', 'r')
	out = open('NodesWithNoEdges.txt','w')
	leftColumn = []
	rightColumn = []

	for line in iter(f):
		temp = []

		x = line.strip()
		x = x.replace("  "," ")
		temp = x.split()

		leftColumn.append(temp[0])
		rightColumn.append(temp[1])

	f.close()
	leftColumn = set(leftColumn)
	lenLeftColumn = len(leftColumn)

	rightColumn = set(rightColumn)
	lenrightColumn = len(rightColumn)

	differenceSet = rightColumn.difference(leftColumn)

	differenceSet = sorted(differenceSet, key=lambda item: (int(item.partition(' ')[0]) if item[0].isdigit() else float('inf'), item))

	for element in differenceSet:
		out.write(str(element))
		out.write("\n")
	print "From Preprocess_76.txt the left column had %s unique nodes and the right column had %s." % (lenLeftColumn, lenrightColumn)
	print "Difference between edges and nodes is %s" % len(differenceSet)

#checkColumnsedges()
checkColumnsPreProcess()