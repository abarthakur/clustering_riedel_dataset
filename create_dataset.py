import gzip
import sys
import os
import csv
import pickle
import SPARQLWrapper
import re
from fuzzywuzzy import fuzz
import time
import codecs
import imp

def load_relation_maps(sparql,NER1,NER2,refresh=False):
    rel_set=set()
    filePath="./data/raw/maps/"
    pickle_dump_path=filePath+NER1.lower()+"-"+NER2.lower()+".p"    
    print(pickle_dump_path)
    if refresh or not os.path.isfile(pickle_dump_path):
        print("Creating new relations set")
        i=0
        stop=False
        typeDict={"PERSON":":people.person","LOCATION":":location.location","ORGANISATION":":organization.organization"}
        pickleFile=open(pickle_dump_path,"wb")
        outFile=open(pickle_dump_path[-1]+".tsv","wb")
        limit=200
        while(not stop):
            print(i)
            query = ('''prefix : <http://rdf.freebase.com/ns/>
                 select distinct ?rel {
                            ?e1 ?rel ?e2 .
                            ?e1 a '''+typeDict[NER1]+" .\n"
                            "?e2 a "+typeDict[NER2]+" \n"
                "} limit "+str(limit)+" offset " +str(i))
            i+=limit
            sparql.setQuery(query)  
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            results= sparql.query().convert()
            print("result size : ",len(results["results"]["bindings"]))
            if (len(results["results"]["bindings"])<limit):
                stop=True
            for result in results["results"]["bindings"]:
                print(result["rel"]["value"].encode('utf-8'))
                if result["rel"]["value"] not in rel_set:
                    rel_set.add(result["rel"]["value"])
                    outFile.write((result["rel"]["value"]+"\n").encode("utf-8"))
        pickle.dump(rel_set,pickleFile)
        pickleFile.close()
        outFile.close()
        return rel_set
    else:
        print("Using existing relation set")
        return pickle.load(open(pickle_dump_path,"rb")) 



def load_entity_map(sparql,entNER,refresh=False):
    entityMap={}
    filePath="./data/raw/maps/"
    pickle_dump_path=filePath+entNER.lower()+".p"    
    if refresh or not os.path.isfile(pickle_dump_path):
        print("Creating new entity map") 
        i=0
        stop=False
        typeDict={"PERSON":":people.person","LOCATION":":location.location","ORGANISATION":":organization.organization"}
        pickleFile=open(pickle_dump_path,"wb")
        outFile=open(filePath+entNER.lower()+".tsv","wb")
        while(not stop):
            print(i)
            query = ('''prefix : <http://rdf.freebase.com/ns/>
                 select distinct ?entity ?entityname{
                            ?entity :type.object.name ?entityname .
                            ?entity a '''+typeDict[entNER]+"\n"
                "} limit 10000 offset " +str(i))
            i+=10000
            sparql.setQuery(query)  
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            results= sparql.query().convert()
            print("result size : ",len(results["results"]["bindings"]))
            if (len(results["results"]["bindings"])<10000):
                stop=True
            for result in results["results"]["bindings"]:
                print(result["entity"]["value"].encode('utf-8'),result["entityname"]["value"].encode('utf-8'))
                if result["entityname"]["value"] not in entityMap:
                    entityMap[result["entityname"]["value"]]=result["entity"]["value"]
                    outFile.write((result["entityname"]["value"]+"\t"+result["entity"]["value"]+"\n").encode("utf-8"))
        pickle.dump(entityMap,pickleFile)
        pickleFile.close()
        outFile.close()
        return entityMap
    else:
        print("Using existing entity map")
        return pickle.load(open(pickle_dump_path,"rb")) 

def load_key_lists(entityMaps):
    print("in")
    t1=time.clock()
    entitySets={}
    for ner in entityMaps:
        keylist=list(entityMaps[ner].keys())
        alphabetsets={}
        for key in keylist:
            if key[0] not in alphabetsets:
                alphabetsets[key[0]]=set()
            alphabetsets[key[0]].add(key)
        entitySets[ner]=alphabetsets
    t2=time.clock()
    print(t2-t1)
    return entitySets


def filter_function(x):
    if x == '' or '\r' in x:
        return False
    else:
        return True

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
        print(result["entity"]["value"],result["entityname"]["value"])
        entMention["fbid"]=result["entity"]["value"]
        entMention["fbname"]=result["entityname"]["value"]


def extractFeatures(sent,relTuple):
    #extract features here. Note sdp algo can be reused easily
    return

def findRelations(relations,sentfile_path,sparql):
    print("Checking "+sentfile_path.split("/")[-1]+" for relations")
    sentfile=open(sentfile_path,"rb")
    while(True):
        sent=load_sentence(sentfile)
        if not sent :
            break
        t1=time.clock()
        checkForRelations(sent,sparql,relations)
        t2=time.clock()
        print(t2-t1)
    sentfile.close()

def checkForRelations(sent,sparql,relations):

    # print "checking for relations"
    for i in range (0,len(sent["mentions"])):
        e1=sent["mentions"][i]
        if not e1["fbid"] :
            continue
        for j in range(0,len(sent["mentions"])):
            e2=sent["mentions"][j]
            if not e2["fbid"] :
                continue
            #query for which relations
            query=('''prefix : <http://rdf.freebase.com/ns/>\nselect distinct ?rel {'''
                    "<"+e1["fbid"].decode('utf-8') +"> ?rel <"+e2["fbid"].decode('utf-8')+">\n}")
            sparql.setQuery(query)
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            results= sparql.query().convert()

            for result in results["results"]["bindings"]:
                print(result["rel"]["value"])
                rel= result["rel"]["value"]
                relTuple=(e1["fbid"],rel,e2["fbid"])
                sentence=sent["words"]
                features=extractFeatures(sent,relTuple)
                relMention={"sourceId":e1["id"],"destId":e2["id"],"sentid":sent["id"],"sentence":sentence,"features":features}
                if relTuple not in relations:
                    relations[relTuple]=[]                    
                relations[relTuple].append(relMention)
    return


def checkForEntities(sent,entityMaps,entitySets):
    idcounter=0
    oldner=""
    validNers=["PERSON","ORGANISATION","LOCATION"]
    mentions=[]
    i=0
    startEntity=1
    oldner="O"
    freebase_count=0
    for ner in sent["ners"]:
        i+=1
        if (oldner!=ner):
            if (oldner in validNers):
                #entity ended
                entMention={}
                entMention["id"]=sent["id"]+"/"+str(idcounter)
                idcounter+=1
                entMention["from"]=startEntity
                entMention["to"]=i-1
                entMention["label"]=oldner
                entMention["string"]=" ".join(sent["words"][startEntity-1:i-1])
                print("New Entity  :  " + str(entMention["string"]))
                entMention["fbid"]=""
                entMention["fbname"]=""
                t1=time.clock()
                found=checkEntityInDict(entMention,entityMaps,entitySets)
                print(time.clock()-t1)
                if found :
                    freebase_count+=1

                mentions.append(entMention)

            if ner in validNers:
                startEntity=i
        oldner=ner

    sent["mentions"]=mentions
    return freebase_count

def checkEntityInDict(entMention,entityMaps,entitySets):
    found=False
    name=entMention["string"]
    label=entMention["label"]
    score=0
    most_probable=""

    if name in entityMaps[label]:
        entMention["fbid"]=entityMaps[label][name]
        entMention["fbname"]=name
        found=True
        print("Freebase entity : "+entMention["fbid"],entMention["fbname"])
        return found
    x=name.split(" ")
    chset=set()
    for word in x:
        chset.add(word[0])
    for ch in chset:
        if ch in entitySets[label]:
            for key in entitySets[label][ch] :#search key list
                if re.match(name,key):
                    score2=fuzz.ratio(key,name)
                    if (score2>score):
                        score=score2
                        most_probable=key

    if (score>0):#perfect match not found
        key=most_probable
        entMention["fbid"]=entityMaps[label][key]
        entMention["fbname"]=key                    
        found=True
        print("Most probable freebase entity : "+entMention["fbid"],entMention["fbname"].encode('utf-8'))
    return found

def write_sentence(sent,sentfile):
    sentfile.write((sent["id"] + "\n").encode('utf-8'))
    sentfile.write(("\t".join(sent["words"])+"\n").encode('utf-8'))
    sentfile.write(("\t".join(sent["tags"])+"\n").encode('utf-8'))
    sentfile.write(("\t".join(sent["ners"])+"\n").encode('utf-8'))
    sentfile.write(("\t".join(sent["depTree"])+"\n").encode('utf-8'))
    sentfile.write(("\t".join(sent["depTreeRels"])+"\n").encode('utf-8'))
    for mention in sent["mentions"]:
        l=[mention["id"],str(mention["from"]),str(mention["to"]),mention["label"],mention["string"],mention["fbid"],mention["fbname"]]
        to_write = "\t".join(l) + "\n"
        sentfile.write(to_write.encode('utf-8'))
    sentfile.write(b"#####\n")

def load_sentence(sentfile):
    sent={}
    check=sentfile.readline().decode("utf-8")[:-1]
    if check == '':
        return None
    sent["id"]=check
    # print check
    sent["words"]=sentfile.readline().decode("utf-8")[:-1]
    sent["tags"]=sentfile.readline().decode("utf-8")[:-1]
    sent["ners"]=sentfile.readline().decode("utf-8")[:-1]
    sent["depTree"]=sentfile.readline().decode("utf-8")[:-1]
    sent["depTreeRels"]=sentfile.readline().decode("utf-8")[:-1]
    sent["mentions"]=[]
    done = False
    while not done :
        line=sentfile.readline().decode("utf-8")[:-1]
        if line == '#####'  :
            done=True
        else:
            mention={}
            [mention["id"],mention["from"],mention["to"],mention["label"],mention["string"],mention["fbid"],mention["fbname"]]=line.split('\t')
            sent["mentions"].append(mention)
    return sent

def warc_to_tsv(warc_file_directory,output_file_directory,start_index,end_index,sparql):
    validNers=["PERSON","ORGANISATION","LOCATION"]
    entityMaps={}
    for ner in validNers:
        entityMaps[ner]=load_entity_map(sparql,ner)
    entitySets=load_key_lists(entityMaps)
    
    sentCount=0
    warc_files=os.listdir(warc_file_directory)
    for i in range(start_index,end_index):
        if i > len(warc_files):
            break
        file_name=warc_files[i]
        print("Processing "+file_name)
        all_fb_sentences=[]
        useless_sentences=[]
        others_sentences=[]
        all_fb_file=open(output_file_directory+file_name+".freebase.tsv","w")
        useless_file=open(output_file_directory+file_name+".useless.tsv","w")
        others_file=open(output_file_directory+file_name+".others.tsv","w")

        f = gzip.open(warc_file_directory + file_name, 'rb')           
        data = list(filter(filter_function, f.read().decode('utf8').split('\n')))
        f.close()

        starting=True
        for line in data :
            '''Each line is of the form - 
                ID FORM POSTAG NERTAG LEMMA DEPREL HEAD SENTID PROV.
            '''
            columns=line.split("\t")
            if columns[0]=="1":
                if not starting:
                    freebase_count=checkForEntities(sent,entityMaps,entitySets)
                    if freebase_count<2:
                        useless_sentences.append(sent)
                        write_sentence(sent,useless_file)
                    elif freebase_count==len(sent["mentions"]):
                        all_fb_sentences.append(sent)
                        write_sentence(sent,all_fb_file)
                    else:
                        others_sentences.append(sent)
                        write_sentence(sent,others_file)
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
          
        all_fb_file.close()
        useless_file.close()
        others_file.close()
        all_fb_pickle=open(output_file_directory+file_name+".freebase.p","wb")
        useless_pickle=open(output_file_directory+file_name+".useless.p","wb")
        others_pickle=open(output_file_directory+file_name+".others.p","wb")
        pickle.dump(all_fb_sentences,all_fb_pickle)
        pickle.dump(useless_fb_sentences,useless_pickle)  
        pickle.dump(others_sentences,others_pickle)  

    print("Finished processing all files")

# if __name__ == "__main__":
#     output_file_directory=sys.argv[2]
#     warc_to_tsv(sys.argv[1],output_file_directory+"sentences/",int(sys.argv[3]),int(sys.argv[4]),sparql)
sparql = SPARQLWrapper.SPARQLWrapper("http://172.16.116.93:8890/sparql/")
# sparql = SPARQLWrapper.SPARQLWrapper("http://172.16.24.160:8890/sparql/")
# validNers=["PERSON","ORGANISATION","LOCATION"]
# entityMaps={}
# for ner in validNers:
#     entityMaps[ner]=load_entity_map(sparql,ner,True)
#imp.reload(sys)  
# print load_key_lists(entityMaps)
#UTF8Writer = codecs.getwriter('utf8')
#sys.stdout = UTF8Writer(sys.stdout)
# t0 = time.time()
# warc_to_tsv()
# rel={}
# findRelations(rel,"./data/raw/output/task_1.warc.gz.freebase.tsv",sparql)
# print rel
# print(time.time() - t0)
# f=open("./data/raw/others_sents.tsv","r")
# print load_sentence(f)
# print f.read()
# print f.readline()
# relPickle=open(,"wb")
# sentfile_path.split("/")[-1][-3]+".relations.p"
# print("Relations"+str(relations))
# pickle.dump(relations,relPickle)
# relPickle.close()
x=["PERSON","LOCATION","ORGANISATION"]
for e1 in x:
    for e2 in x:
        load_relation_maps(sparql,e1,e2,True)