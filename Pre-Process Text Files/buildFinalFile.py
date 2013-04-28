import os
import string
import pprint as pp

def buildNodeWDict():
	print "Entering buildNodeWDict Function"
	edgeNodes = {}
	f = open('Preprocess_76.txt', 'r')

	for x in range(0,685230):
		edgeNodes[str(x)] = []

	for line in iter(f):
		theList = []

		x = line.strip()
		x = x.replace("  "," ")
		theList = x.split()

		edgeNodes[str(theList[0])].append(theList[1])

	f.close()
	#pp.pprint(edgeNodes)
	return edgeNodes

def buildDegreesDictionary():
	print "Entering buildDegreesDictionary Function"
	dd = open('DegreesOfNodes.txt', 'r')
	dict = {}
	for line in iter(dd):
		temp = line.strip().split()
		dict[temp[0]] = temp[1]
	#pp.pprint(dict)
	dd.close()
	return dict

def loadPreProcessFile(leDict,edgeNodes):
	print "Entering loadPreProcessFile Function"
	f = open('Preprocess_76.txt', 'r')
	finalFile = open('PreprocessFinalFile.txt','w')
	
	lastTime = ""
	for line in iter(f):
		theList = []

		x = line.strip()
		x = x.replace("  "," ")
		theList = x.split()



		if str(theList[0]) == lastTime:
			print "Skipped a %s line" % str(theList[0])
			pass
		else:
			#print theList[0]

			#Add Degrees To The x array
			theList.append(leDict[str(theList[0])])
				#Format is now ['node','other node','rand number','degrees(node)']

			#Compute PageRank for Node
			pageRank = 1.0/float(theList[3])

			#Build Array In String Format
			arraryString = ""
			for x in range(0,len(edgeNodes[str(theList[0])])):
				arraryString = arraryString + edgeNodes[str(theList[0])][x] + ","

			arraryString = arraryString[0:-1]

			#Build Output String
				#Format ['node','PageRank Estimate','degrees(node)',['other nodes tuple']]
			finalString = str(theList[0]) + " " + str(pageRank) + " " + str(theList[3]) + " " + arraryString

			lastTime = str(theList[0])

			#Write To The FinalFile
			finalFile.write(finalString)
			finalFile.write("\n")

	f.close()
	finalFile.close()


loadPreProcessFile(buildDegreesDictionary(),buildNodeWDict())
print ">>>>DONE!<<<<"