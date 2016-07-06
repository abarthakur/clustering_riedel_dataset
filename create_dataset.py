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
        if (oldner!=ner):
            if (oldner in validNers):
                #entity ended
                entMention={}
                entMention["id"]=idcounter
                idcounter+=1
                entMention["from"]=startEntity
                entMention["to"]=i-1
                entMention["label"]=oldner
                entMention["string"]=sent["words"][startEntity-1:i-1]
                print "New Entity  :  " + str(entMention["string"])
                checkEntityFreebase(entMention,sparql)
                mentions.append(entMention)
            if ner in validNers:
                startEntity=i
        oldner=ner

    sent["mentions"]=mentions

def checkEntityFreebase(entMention,sparql):
    '''query of the form -
            select ?entity ?entityname{
            ?entity :type.object.name ?entityname .
            ?entity a :people.person .
            filter regex(str(?entityname), "barack.*obama","i")
        } limit 1
    '''
    entMention["fbid"]=None
    regex= " ".join(entMention["string"])
    typeDict={"PERSON":":people.person","LOCATION":":location.location","ORGANISATION":":organization.organization"}
    #query freebase and set Guid if any. set to none else.
    query = ('''prefix : <http://rdf.freebase.com/ns/>\n select ?entity ?entityname{
            ?entity :type.object.name ?entityname .
            ?entity a '''+typeDict[entMention["label"]]+" .\n"
            '''filter regex(str(?entityname),"'''+regex+'''","i")\n'''
            "} limit 1")
    # print query
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    results= sparql.query().convert()
    for result in results["results"]["bindings"]:
        print result["entity"]["value"],result["entityname"]["value"]
        entMention["fbid"]=result["entity"]["value"]
        entMention["fbname"]=result["entityname"]["value"]


def extractFeatures(sent,relTuple):
    #extract features here. Note sdp algo can be reused easily
    return



def checkForRelations(sent,sparql,file_name,sentCount,relations):
    ''' query of the form -
        select distinct ?rel {
        <http://rdf.freebase.com/ns/m.01146ccd> ?rel <http://rdf.freebase.com/ns/m.010lsl_x>
        }
    '''
    print "checking for relations"
    for i in range (0,len(sent["mentions"])):
        e1=sent["mentions"][i]
        if not e1["fbid"] :
            continue
        for j in range(i+1,len(sent["mentions"])):
            e2=sent["mentions"][j]
            if not e2["fbid"] :
                continue
            #query for which relations
            query=('''prefix : <http://rdf.freebase.com/ns/>\nselect distinct ?rel {'''
                    "<"+e1["fbid"]+"> ?rel <"+e2["fbid"]+">\n}")
            sparql.setQuery(query)
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            results= sparql.query().convert()

            for result in results["results"]["bindings"]:
                print result["rel"]["value"]
                rel= result["rel"]["value"]
                relTuple=(e1["fbid"],rel,e2["fbid"])
                sentence=" ".join(sent["words"])
                features=extractFeatures(sent,relTuple)
                relMention={"sourceId":e1["id"],"destId":e2["id"],"filename":file_name,"sentence":sentence,"features":features}
                if relTuple not in relations:
                    relations[relTuple]=[]                    
                relations[relTuple].append(relMention)


    return




''' doc : dictionary representing a single warc file. It's keys -
        filename : it's filename (string)
        sentences : list of sentence dictionaries corresponding to sentences in the document 
    sentence : a dictionary of values representing sentences. It's keys-
        words :
        tags : POS tags
        ners :NER tags
        depTree: list of integers representing the dependency tree (heads of edges)
        depTreeRels: list of relations corresponding to the relations of the dep tree
        mentions : list of entity mention dictionaries 
    entMention : a dictionary representing an entity mention. It's keys-
        id,from,to,label,string
        fbid,fbname : the freebase uri and freebase name respecively
    relations : a dictionary with keys of the form of a tuple of (e1.fbid,URI of rel,e2.fbid)
                values are relMention dictionaries
    relMention : a dictionary with keys-
                sourceId,destId,filename,sentence,features
'''

sparql = SPARQLWrapper.SPARQLWrapper("http://172.16.116.93:8890/sparql/")
warc_file_directory     = "./data/raw/wiki_teaser/"
pickle_directory      = "./data/raw/pickle_full/"
docCount=0
sentCount=0
idcounter=0
relations={}
for file_name in os.listdir(warc_file_directory):
    f = gzip.open(warc_file_directory + file_name, 'rb')
    data = filter(filter_function, f.read().decode('utf8').split('\n'))
    sentences=[]
    starting=True
    for line in data :
        '''Each line is of the form - 
            ID FORM POSTAG NERTAG LEMMA DEPREL HEAD SENTID PROV.
        '''
        columns=line.split("\t")
        if columns[0]=="1":
            if not starting:
                sent["depTree"]=depTree
                sent["depTreeRels"]=depTreeRels
                checkForEntities(sent,sparql,idcounter)
                checkForRelations(sent,sparql,file_name,sentCount,relations)
                sentences.append(sent)
            else :
                starting=False

            sent={}
            depTree=[]
            depTreeRels=[]
            sent["words"]=[]
            sent["tags"]=[]
            sent["ners"]=[]
            sentCount+=1

        
        sent["words"].append(columns[1])
        sent["tags"].append(columns[2])
        sent["ners"].append(columns[3])
        depTree.append(columns[6])
        depTreeRels.append(columns[5])
      
    # break
    doc={}
    doc["filename"]=file_name
    doc["sentences"]=sentences
    outFile=open(pickle_directory+file_name+".p", "wb")
    pickle.dump(doc, outFile)
    outFile.close()
    docCount+=1

relPickle=open("./data/raw/relations.p","wb")
print "Relations"+str(relations)
pickle.dump(relations,relPickle)
relPickle.close()