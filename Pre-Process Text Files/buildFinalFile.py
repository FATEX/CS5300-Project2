import os
import string
import pprint as pp

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

def loadPreProcessFile(leDict):
	print "Entering loadPreProcessFile Function"
	f = open('Preprocess_76.txt', 'r')
	finalFile = open('PreprocessFinalFile.txt','w')
	for line in iter(f):
		theList = []

		x = line.strip()
		x = x.replace("  "," ")
		theList = x.split()

		#print theList[0]

		#Add Degrees To The x array
		theList.append(leDict[str(theList[0])])
			#Format is now ['node','other node','rand number','degrees(node)']

		#Compute PageRank for Node
		pageRank = 1.0/float(theList[3])

		#Build Output String
			#Format ['node','PageRank Estimate','degrees(node)','other node']
		finalString = str(theList[0]) + " " + str(pageRank) + " " + str(theList[3]) + " " + str(theList[1])

		#Write To The FinalFile
		finalFile.write(finalString)
		finalFile.write("\n")
	f.close()
	finalFile.close()


loadPreProcessFile(buildDegreesDictionary())
print ">>>>DONE!<<<<"