import Document_pb2
import re
import pickle
import sys  




def createGuidMap():
	freebaseFile=open("./data/raw/filtered-freebase-simple-topic-dump-3cols.tsv","r")
	guidMap={}
	pattern=r'(/guid/[a-zA-Z0-9]*)([^/]*).*'
	print pattern
	for line in freebaseFile:
		#print line

		mo=re.match(pattern,line)
		guid=mo.group(1)
		ent=mo.group(2)
		ent=ent.strip()
		print guid
		print ent
		guidMap[guid]=ent

	pickle.dump(guidMap,open("./guidMap.p","wb"))

def loadGuidMap():
	return pickle.load(open("./guidMap.p","rb"))

def createVectorMap():
	vecFile= open("./data/raw/glove.6B/glove.6B.50d.txt","r")
	vectorMap={}
	for line in vecFile:
		tokenList=line.split(" ")
		word = tokenList[0] 
		vector=[]
		for value in tokenList[1:]:
			vector.append(float(value))
		vectorMap[word]=vector
		# break
		print word

	pickle.dump(vectorMap,open("./vectorMap.p","wb"))

	# for x in vectorMap["the"]:
	# 	print x

def loadVectorMap():
	return pickle.load(open("./vectorMap.p","rb"))



def writeBuckets_1(guidMap):
	reload(sys)  
	sys.setdefaultencoding('utf8')
	rel=Document_pb2.Relation()
	errorfile = open("err.txt","w")
	bucketLabelFile=open("./data/processed/bucket_labels.tsv","w")
	relIDFile=open("./data/processed/relationIDs.tsv","w")
	relIDMap={}
	relCount=0
	for i in range(100000,119465):
	# for i in range(100000,100002):
		print i
		inputFile= open("./data/raw/kb_manual/trainPositive/"+str(i)+".pb","rb")
		rel.ParseFromString(inputFile.read())
		#print rel
		# break
		#print rel.sourceGuid + " " + rel.destGuid 
		inputFile.close()

		relType=rel.relType
		print relType
		if relType not in relIDMap:
			relIDFile.write(str(relCount)+"\t"+relType+"\n")
			relIDMap[relType]=relCount
			relCount+=1
		
		bucketLabelFile.write(str(relIDMap[relType])+"\n")

		outputFile = open("./data/processed/buckets/b_"+str(i)+".tsv","w")
		# print rel.sourceGuid
		sourceEntity= guidMap[rel.sourceGuid]
		# print sourceEntity
		# print rel.destGuid
		destEntity=guidMap[rel.destGuid]
		# print destEntity

		# sourceEntity="a"
		# destEntity="b"


		for mention in rel.mention :
			sent=mention.sentence
			#print mention.sourceId
			# print sent
			phrase1=re.match("(.*)"+sourceEntity+"(.*)"+destEntity+"(.*)",sent)
			phrase2=re.match("(.*)"+destEntity+"(.*)"+sourceEntity+"(.*)",sent)
			if phrase1:
				phrase=phrase1.group(2)
			elif phrase2:
				phrase=phrase2.group(2)
			else:
				phrase = "Not FOUND"
				errorfile.write(str(i)+"\n")
			phrase=phrase.strip()
			string =sourceEntity+"\t"+destEntity+"\t"+phrase+"\t"+sent+"\n"
			outputFile.write(string);
		outputFile.close()
		# print rel
	bucketLabelFile.close()
	errorfile.close()
	relIDFile.close()







createGuidMap()
guidMap=loadGuidMap()
# guidMap=None
writeBuckets_1(guidMap)
# rel=Document_pb2.Relation()
# rel.ParseFromString(open("./kb_manual/trainPositive/"+"100008"+".pb","rb").read())
# sourceEntity= guidMap[rel.sourceGuid]
# # print sourceEntity
# # print rel.destGuid
# destEntity=guidMap[rel.destGuid]
# print rel.mention[0].sentence
createVectorMap()
print "My watch has ended"
	