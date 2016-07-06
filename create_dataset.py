import gzip
import sys
import os
import csv
import Document_pb2
import pickle
import SPARQLWrapper

def filter_function(x):
    if x == '' or '\r' in x:
        return False
    else:
        return True

def checkForEntities(sent,sparql,idcounter):
    print "checking for entities"
    oldner=""
    validNers=["PERSON","ORGANISATION","LOCATION"]
    mentions=[]
    i=0
    startEntity=1
    oldner="O"
    for ner in sent["ners"]:
        i+=1
        print ner,i
        if (oldner!=ner):
            print "ner change",i
            if (oldner in validNers):
                #entity ended
                print "ner end",i
                entMention={}
                entMention["id"]=idcounter
                idcounter+=1
                entMention["from"]=startEntity
                entMention["to"]=i-1
                entMention["label"]=oldner
                entMention["string"]=" ".join(sent["words"][startEntity-1:i-1])
                print "New Entity  :  " + entMention["string"]
                # checkEntityFreebase(entMention,sparql)
                mentions.append(entMention)
            if ner in validNers:
                startEntity=i
        oldner=ner

    sent["mentions"]=mentions

def checkEntityFreebase(entMention,sparql):
    foundEntity=False
    #query freebase and set Guid if any. set to none else.
    if not foundEntity:
        entMention["guid"]=None
    return

def extractFeatures(sent,relTuple):
    #extract features here. Note sdp algo can be reused easily
    return



def checkForRelations(sent,sparql,file_name,sentCount,relations):

    for i in range (0,len(sent["mentions"])):
        e1=sent["mentions"][i]
        if not e1 :
            continue
        for j in range(i+1,len(sent["mentions"])):
            foundRelation=False #dummy booleans later replace with proper condition
            e2=sent["mentions"][j]
            if not e2 :
                continue
            #query for which relations
            #check if relations match training set
            if foundRelation :
                for rel in returnedRels:
                    relTuple=(e1["guid"],rel,e2["guid"])
                    sentence=" ".join(sent["words"])
                    features=extractFeatures(sent,relTuple)
                    relMention={"sourceId":e1["id"],"destId":e2["id"],"filename":file_name,"sentence":sentence,"features":features}
                    relations[relTuple].append(relMention)


    return



# ID FORM POSTAG NERTAG LEMMA DEPREL HEAD SENTID PROV.
query = """ SELECT * FROM NAMED <http://freebase.com> WHERE {?s ?x ?o} limit 100 """
sparql = SPARQLWrapper.SPARQLWrapper("http://172.16.116.93:8890/sparql/")
# sparql.setQuery(query)
# sparql.setReturnFormat(SPARQLWrapper.JSON)
# results= sparql.query().convert()
warc_file_directory     = "./data/raw/wiki_teaser/"
pickle_directory      = "./data/raw/pickle_full/"
docCount=0
sentCount=0
idcounter=0
#will we run out of memory?
relations={}
for file_name in os.listdir(warc_file_directory):
    f = gzip.open(warc_file_directory + file_name, 'rb')
    data = filter(filter_function, f.read().decode('utf8').split('\n'))
    sentences=[]
    starting=True
    for line in data :
        columns=line.split("\t")
        if columns[0]=="1":
            if not starting:
                sent["depTree"]=depTree
                sent["depTreeRels"]=depTreeRels
                checkForEntities(sent,sparql,idcounter)
                # checkForRelations(sent,sparql,file_name,sentCount,relations)
                sentences.append(sent)
                # print "old sentence :"+str(sent)            
            else :
                starting=False

            sent={}
            depTree=[]
            depTreeRels=[]
            sent["words"]=[]
            sent["tags"]=[]
            sent["ners"]=[]
            sentCount+=1

            # print "new sentence"

        #continue
        
        sent["words"].append(columns[1])
        sent["tags"].append(columns[2])
        sent["ners"].append(columns[3])
        depTree.append(columns[6])
        depTreeRels.append(columns[5])
        # print "token"
      
    # break
    doc={}
    doc["filename"]=file_name
    doc["sentences"]=sentences
    outFile=open(pickle_directory+file_name+".p", "wb")
    # pickle.dump(doc, outFile)
    outFile.close()
    docCount+=1

# relPickle=open("./data/raw/relations.p","wb")
# # print "Relations"+str(relations)
# pickle.dump(relations,relPickle)
# relPickle.close()