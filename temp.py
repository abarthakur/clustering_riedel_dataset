import Document_pb2	
import reader2
from collections import deque

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
	print "Before1",adjlist[24]
	for i in range(e1[0]+1,e1[1]+1):
		if (adjlist[i][0]!=e1[0]):
			adjlist[e1[0]].extend(adjlist[i])
	
	print "Before2",adjlist[24]
	for i in range(e2[0]+1,e2[1]+1):
		if (adjlist[i][0]!=e2[0]):
			adjlist[e2[0]].extend(adjlist[i])

	#make unweighted 
	

	# for i in range(1,len(treeList)+1):
	# 	if(treeList[i-1]==0) :
	# 		adjlist[i]=[]
	# 		continue
	# 	adjlist[i]=[treeList[i-1]]
	print "Before3",adjlist[24]
	for key in adjlist :
		for neighbour in adjlist[key]:
			# print "#####\n"+str(key)
			# print type(neighbour)
			# print neighbour
			if key not in adjlist[neighbour]:
				adjlist[neighbour].append(key)
	print "After3",adjlist[24]
	#do bfs

	q= deque()
	q.append(e1[0])
	visited={}
	pred = {}
	pred[e1[0]]=None
	visited[e1[0]]=True
	while True:
		try:
			u=q.popleft()
		except IndexError:
			break
		print str(u) + ": Popped!"
		print adjlist[u]
		for v in adjlist[u]:
			try :
				t=visited[v]
				print str(v) + ": Visited already"
			except KeyError:
				print  str(v) +": Newly visited"
				q.append(v)
				visited[v]=True
				pred [v]= u 

	#########
	print "\n"
	for i in range(1,len(treeList)+1):
		# print visited[i]
		print str(i) +": " + str(pred[i])

	sdpath=[]
	v=e2[0]
	print v
	print pred[v]
	print "Collecting path"
	while pred[v]:
		v=pred[v]
		print pred[v],toklist[v-1]
		sdpath.append(toklist[v-1])

	return sdpath[:-1]


# treeList = [0,11,2,7,8,8,1,10,2,1,6]
# toklist = ['A','B','C','D','E','F','G','H','I','J','K']
# e1=(1,2)
# e2=(5,6)
# print "#####\n" + str(shortestDP(treeList,e1,e2,toklist))






sample_file_path = "./data/raw/kb_manual/trainPositive/"
docFilePath="./data/raw/nyt-2005-2006.backup/"
rel=Document_pb2.Relation()
mentionDoc=Document_pb2.Document()

######
i=100000
######Loop1 begins
######CP
inputFile= open(sample_file_path + str(i) + ".pb","rb")
rel.ParseFromString(inputFile.read())
inputFile.close()

for mention in rel.mention :
######/CP
	print 1
	mentionFile=open(docFilePath+mention.filename.split('/')[-1])
	mentionDoc.ParseFromString(mentionFile.read())
	mentionFile.close()
	#match rel.sentence in document
	for sent in mentionDoc.sentences :
		toklist=[]
		for token in sent.tokens:
			toklist.append(token.word)
		print toklist
		#if matched
		if toklist==mention.sentence.strip().split(" ") :
			# print 4
			root=sent.depTree.root
			treeList=sent.depTree.head
			#find the entity ranges
			for entMention in sent.mentions :
				# print 5
				if (mention.sourceId == entMention.id):
					e1 = (getattr(entMention,"from"), entMention.to )
				elif(mention.destId==entMention.id):
					e2 = (getattr(entMention,"from"), entMention.to )
			#find the dependency parse
			print e1,e2
			# for i in range (0,len(treeList)):
			# 	print str(i+1) +"-->" + str(treeList[i])
			sdp=shortestDP(treeList,e1,e2,toklist)
			print sdp
			break
	break

