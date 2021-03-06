import Document_pb2	
from collections import deque
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def shortestDP(treeList,e1,e2,toklist):
	#preprocess
	adjlist={}

	#merge vertices
	for i in range(0,len(treeList)):
		x=treeList[i]
		if x==0 :
			adjlist[i+1]=[]
			continue
		elif x in range(e1[0],e1[1]+1):
			y=e1[0]
		elif x in range(e2[0],e2[1]+1):
			y=e2[0]
		else :
			y=x
		adjlist[i+1]=[]
		adjlist[i+1].append(y)

	for i in range(e1[0]+1,e1[1]+1):
		if (len(adjlist[i])>0 and adjlist[i][0]!=e1[0]): #skip the first word, skip words which are roots
			adjlist[e1[0]].extend(adjlist[i])
	
	for i in range(e2[0]+1,e2[1]+1):
		if (len(adjlist[i])>0 and adjlist[i][0]!=e2[0]):
			adjlist[e2[0]].extend(adjlist[i])

	#make undirected 
	for key in adjlist :
		for neighbour in adjlist[key]:
			# print "#####\n"+str(key)
			# print type(neighbour)
			# print neighbour
			if key not in adjlist[neighbour]:
				adjlist[neighbour].append(key)

	#do bfs
	q= deque()
	q.append(e2[0])
	visited={}
	pred = {}
	pred[e2[0]]=None
	visited[e2[0]]=True
	while True:
		try:
			u=q.popleft()
		except IndexError:
			break
		# print str(u) + ": Popped!"
		# print adjlist[u]
		for v in adjlist[u]:
			try :
				t=visited[v]
				# print str(v) + ": Visited already"
			except KeyError:
				# print  str(v) +": Newly visited"
				q.append(v)
				visited[v]=True
				pred [v]= u 

	# print "\nPredecessors\n"
	# for i in range(1,len(treeList)+1):
	# 	try :
	# 		# print visited[i]
	# 		print str(i) +": " + str(pred[i])
	# 	except KeyError:
	# 		print str(i) +": Unvisited"

	sdpath=[]
	v=e1[0]
	# print "Collecting path"
	try:
		while pred[v]:
			v=pred[v]
			# print v,pred[v],toklist[v-1]
			sdpath.append(toklist[v-1])
	except KeyError:#disjoint case
		return []

	return sdpath[:-1]


def findSDP(rel,mention,mentionDoc,docFilePath):
	''' return type : Pair
		0th index : Boolean indicating success of operation
		1st index : String of the SDP. Direction is from e1 to e2. Does not include the entities.
	'''
	mentionFile=open(docFilePath+mention.filename.split('/')[-1])
	mentionDoc.ParseFromString(mentionFile.read())
	mentionFile.close()
	
	error_msg=""
	#match rel.sentence in document
	for sent in mentionDoc.sentences :
		toklist=[]
		for token in sent.tokens:
			toklist.append(token.word)
		
		if toklist==mention.sentence.strip().split(" ") :

			treeList=sent.depTree.head
			#find the entity ranges
			e1=None
			e2=None
			for entMention in sent.mentions :

				if (mention.sourceId == entMention.id or rel.sourceGuid == entMention.entityGuid):
					e1 = (getattr(entMention,"from")+1, entMention.to+1 )
				elif(mention.destId==entMention.id or rel.destGuid == entMention.entityGuid):
					e2 = (getattr(entMention,"from")+1, entMention.to+1 )
			#find the dependency parse
			if (len(treeList)==0):
				error_msg="No Dep Tree found"
				break

			# for i in range (0,len(treeList)):
			# 	print str(i+1) +"-->" + str(treeList[i])
			if e1 and e2 :
				return (True," ".join(shortestDP(treeList,e1,e2,toklist)))
			else:
				error_msg="Entity mention did not match"

	return (False,error_msg)

if __name__ == "__main__":
	sample_file_path = "../data/external/kb_manual/trainPositive/"
	docFilePath="../data/external/nyt-2005-2006.backup/"
	rel=Document_pb2.Relation()
	mentionDoc=Document_pb2.Document()

	######
	sample_low_count = 100000
	# sample_high_count = 101879
	sample_high_count = 119465
	# sample_high_count= 100100
	badcount=0
	for i in range(sample_low_count,sample_high_count):
		print "Rel No : "+str(i)
		inputFile= open(sample_file_path + str(i) + ".pb","rb")
		rel.ParseFromString(inputFile.read())
		inputFile.close()

		for mention in rel.mention :
			print inputFile
			print "SDP is : "+str(findSDP(rel,mention,mentionDoc,docFilePath))
		######/CP


