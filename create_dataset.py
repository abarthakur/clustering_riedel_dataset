import gzip
import sys
import os
import csv
import Document_pb2
import pickle
import SPARQLWrapper
import re
from fuzzywuzzy import fuzz


def filter_function(x):
    if x == '' or '\r' in x:
        return False
    else:
        return True

def checkForEntities(sent,idcounter,entityMaps):
    print "checking for entities"
    oldner=""
    validNers=["PERSON","ORGANISATION","LOCATION"]
    mentions=[]
    i=0
    startEntity=1
    oldner="O"
    allfreebase=True
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
                entMention["string"]=" ".join(sent["words"][startEntity-1:i-1])
                print "New Entity  :  " + str(entMention["string"])
                # checkEntityFreebase(entMention,sparql)
                entMention["fbid"]=""
                entMention["fbname"]=""
                found=checkEntityInDict(entMention,entityMaps)
                if not found :
                    allfreebase=False
                #
                mentions.append(entMention)


            if ner in validNers:
                startEntity=i
        oldner=ner
    sent["mentions"]=mentions
    return allfreebase

def checkEntityInDict(entMention,entityMaps):
    found=False
    name=entMention["string"]
    label=entMention["label"]
    score=0
    most_probable=""
    # threshold=95
    
    if name in entityMaps[label]:
        #should we use casefold or normalize to deal with unicode data?
        entMention["fbid"]=entityMaps[label][name]
        entMention["fbname"]=name
        found=True
        print "Freebase entity : "+entMention["fbid"],entMention["fbname"]
        return found

    for key in entityMaps[label] :#search key list
        if re.match(name,key):
            score2=fuzz.ratio(key,name)
            if (score2>score):
                score=score2
                most_probable=key
            # if (score > threshold):
            #     most_probable=key
            #     break

    if (score>0):#perfect match not found
        key=most_probable
        entMention["fbid"]=entityMaps[label][key]
        entMention["fbname"]=key                    
        found=True
        print "Most probable freebase entity : "+entMention["fbid"],entMention["fbname"]
    return found


def checkEntityFreebase(entMention,sparql):
    '''query of the form -
            select ?entity ?entityname{
            ?entity :type.object.name ?entityname .
            ?entity a :people.person .
            filter regex(str(?entityname), "barack.*obama","i")
        } limit 1
    '''
    entMention["fbid"]=None
    regex= entMention["string"]
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

def findRelations(sentfile,sparql):
    print "Checking for relations"
    relations={}
    while(True):
        sent=load_sentence(sentfile)
        if not sent :
            break
        checkForRelations(sent,sparql,relations)
    relPickle=open("./data/raw/relations.p","wb")
    print "Relations"+str(relations)
    pickle.dump(relations,relPickle)
    relPickle.close()


def checkForRelations(sent,sparql,relations):

    # print "checking for relations"
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
                relMention={"sourceId":e1["id"],"destId":e2["id"],"sentid":sent["id"],"sentence":sentence,"features":features}
                if relTuple not in relations:
                    relations[relTuple]=[]                    
                relations[relTuple].append(relMention)
    return

def write_sentence(sent,sentfile):
    sentfile.write(sent["id"]+"\n")
    sentfile.write("\t".join(sent["words"])+"\n")
    sentfile.write("\t".join(sent["tags"])+"\n")
    sentfile.write("\t".join(sent["ners"])+"\n")
    sentfile.write("\t".join(sent["depTree"])+"\n")
    sentfile.write("\t".join(sent["depTreeRels"])+"\n")
    for mention in sent["mentions"]:
        l=[str(mention["id"]),str(mention["from"]),str(mention["to"]),mention["label"],mention["string"],mention["fbid"],mention["fbname"]]
        sentfile.write("\t".join(l)+"\n")
    sentfile.write("#####\n")

def load_sentence(sentfile):
    sent={}
    check=sentfile.readline()[:-1]
    if check=="":
        return None
    sent["id"]=check
    # print check
    sent["words"]=sentfile.readline()[:-1]
    sent["tags"]=sentfile.readline()[:-1]
    sent["ners"]=sentfile.readline()[:-1]
    sent["depTree"]=sentfile.readline()[:-1]
    sent["depTreeRels"]=sentfile.readline()[:-1]
    sent["mentions"]=[]
    done = False
    while not done :
        line=sentfile.readline()[:-1]
        if line == "#####"  :
            done=True
        else:
            mention={}
            [mention["id"],mention["from"],mention["to"],mention["label"],mention["string"],mention["fbid"],mention["fbname"]]=line.split("\t")
            sent["mentions"].append(mention)
    return sent



def warc_to_tsv():
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
    validNers=["PERSON","ORGANISATION","LOCATION"]
    entityMaps={}
    for ner in validNers:
        entityMaps[ner]=load_entity_map(sparql,ner)
    warc_file_directory     = "./data/raw/wiki_teaser/"
    pickle_directory      = "./data/raw/pickle_full/"
    freebase_file=open("./data/raw/allfreebase_sents.tsv","w")
    others_file=open("./data/raw/others_sents.tsv","w")
    docCount=0
    sentCount=0
    idcounter=0
    # relations={}
    only_fb_sentences=[]
    others_sentences=[]
    for file_name in os.listdir(warc_file_directory):
        f = gzip.open(warc_file_directory + file_name, 'rb')
        data = filter(filter_function, f.read().decode('utf8').split('\n'))
        starting=True
        for line in data :
            '''Each line is of the form - 
                ID FORM POSTAG NERTAG LEMMA DEPREL HEAD SENTID PROV.
            '''
            columns=line.split("\t")
            if columns[0]=="1":
                if not starting:

                    allfreebase=checkForEntities(sent,idcounter,entityMaps)
                    if not allfreebase:
                        only_fb_sentences.append(sent)
                        write_sentence(sent,freebase_file)
                    else:
                        others_sentences.append(sent)
                        write_sentence(sent,others_file)
                    # checkForRelations(sent,sparql,file_name,sentCount,relations)

                else :
                    starting=False

                sent={}
                depTree=[]
                depTreeRels=[]
                sent["words"]=[]
                sent["tags"]=[]
                sent["ners"]=[]
                sent["depTree"]=[]
                sent["depTreeRels"]=[]
                sentCount+=1


            sent["id"]=file_name+"/"+str(sentCount)
            sent["words"].append(columns[1])
            sent["tags"].append(columns[2])
            sent["ners"].append(columns[3])
            sent["depTree"].append(columns[6])
            sent["depTreeRels"].append(columns[5])  
          
        # break
        # doc={}
        # doc["filename"]=file_name
        # doc["sentences"]=sentences
        # outFile=open(pickle_directory+file_name+".p", "wb")
        # pickle.dump(doc, outFile)
        # outFile.close()
        docCount+=1
    pickle.dump(only_fb_sentences,open("./data/raw/allfreebase_sents.p","wb"))
    pickle.dump(only_fb_sentences,open("./data/raw/others_sents.p","wb"))  
    # relPickle=open("./data/raw/relations.p","wb")
    # print "Relations"+str(relations)
    # pickle.dump(relations,relPickle)
    # relPickle.close()

def load_entity_map(sparql,entNER,refresh=False):
    entityMap={}
    filePath="./data/raw/maps/"
    files={"PERSON":"persons","ORGANISATION":"organisations","LOCATION":"locations"}
    pickle_dump_path=filePath+files[entNER]+".p"    
    if refresh or not os.path.isfile(pickle_dump_path):
        print "Creating new entity map" 
        i=0
        stop=False
        typeDict={"PERSON":":people.person","LOCATION":":location.location","ORGANISATION":":organization.organization"}
        pickleFile=open(pickle_dump_path,"wb")
        outFile=open(filePath+files[entNER]+".tsv","w")
        while(not stop):
            print i
            query = ('''prefix : <http://rdf.freebase.com/ns/>
                 select ?entity ?entityname{
                            ?entity :type.object.name ?entityname .
                            ?entity a '''+typeDict[entNER]+"\n"
                "} limit 10000 offset " +str(i))
            i+=10000
            sparql.setQuery(query)  
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            results= sparql.query().convert()
            print "result size : ",len(results["results"]["bindings"])
            if (len(results["results"]["bindings"])<10000):
                stop=True
            for result in results["results"]["bindings"]:
                print result["entity"]["value"],result["entityname"]["value"]
                entityMap[result["entityname"]["value"]]=result["entity"]["value"]
                outFile.write((result["entityname"]["value"]+"\t"+result["entity"]["value"]+"\n").encode("utf-8"))
        pickle.dump(entityMap,pickleFile)
        pickleFile.close()
        outFile.close()
        return entityMap
    else:
        print "Using existing entity map"
        return pickle.load(open(pickle_dump_path,"rb")) 


# sparql = SPARQLWrapper.SPARQLWrapper("http://172.16.116.93:8890/sparql/")
# validNers=["PERSON","ORGANISATION","LOCATION"]
# for ner in validNers:
#     load_entity_map(sparql,ner)
reload(sys)  
sys.setdefaultencoding('utf8')
warc_to_tsv()
findRelations(open("./data/raw/others_sents.tsv","r"),sparql)
# f=open("./data/raw/others_sents.tsv","r")
# print load_sentence(f)
# print f.read()
# print f.readline()