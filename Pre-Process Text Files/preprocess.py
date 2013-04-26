import os
import string

def computeRejectValues():
	fromNetID = 0.76
	rejectMin = 0.99 * fromNetID;
	rejectLimit = rejectMin + 0.01;
	#print "Reject Min: %f" %  rejectMin
	#print "Reject Limit: %f" % rejectLimit

	return rejectMin, rejectLimit

def main(rejectMin, rejectLimit):
	rejectMin = rejectMin
	rejectLimit = rejectLimit
	#print "Reject Min: %f" %  rejectMin
	#print "Reject Limit: %f" % rejectLimit

	print "Entering Main Function"
	f = open('edges.txt')
	outFile = open("Preprocess_76.txt", "w")
	outFileRejection = open("Preprocess_76_Rejects.txt", "w")
	removeCounter = 0.0
	overallCounter = 0.0
	for line in iter(f):
		overallCounter += 1
		temp = line.split()
		holder = temp[2]
		floatRandomValue = (float)(holder)
		if floatRandomValue >= rejectMin and floatRandomValue <= rejectLimit:
			#print "Removed a line!"
			removeCounter += 1
			outFileRejection.write(line)
		else:
			outFile.write(line)
	
	print "Overall counter: %d" % overallCounter
	print "Remove counter: %d" % removeCounter
	removeRate = (removeCounter/overallCounter)*100
	print "Remove Rate =  %f" % removeRate 
	f.close()

k, v = computeRejectValues()
main(k, v)