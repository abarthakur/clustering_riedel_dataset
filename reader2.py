import Document_pb2
import re
import pickle
import sys  
import numpy as np
import os.path
from matplotlib import pyplot as plt
from scipy.cluster.hierarchy import dendrogram, linkage, cophenet
from scipy.spatial.distance import pdist

def loadGuidMap(refresh=False):
    pickle_dump_path = "./data/interim/guidMap.p"
    guid_file_path = "./data/raw/filtered-freebase-simple-topic-dump-3cols.tsv"
    if refresh or not os.path.isfile(pickle_dump_path):
        print "Creating new guid map pickle dump"
        guidFile=open(guid_file_path,"r")
        guidMap={}
        pattern=r'(/guid/[a-zA-Z0-9]*)([^/]*).*'
        for line in guidFile:
            mo=re.match(pattern,line)
            guid=mo.group(1)
            ent=mo.group(2)
            ent=ent.strip()
            guidMap[guid]=ent
        pickle.dump(guidMap,open(pickle_dump_path,"wb"))
    else:
        print "Using existing pickle dump for guidMap"
        guidMap = pickle.load(open(pickle_dump_path, "rb"))

    return guidMap

def loadVectorMap(refresh=False):
    pickle_dump_path = "./data/interim/vectorMap.p"
    vector_file_path = "./data/raw/glove.6B/glove.6B.50d.txt"
    if refresh or not os.path.isfile(pickle_dump_path):
        print "Creating new vector map" 
        vecFile= open(vector_file_path,"r")
        vectorMap={}
        for line in vecFile:
            tokenList=line.split(" ") 
            word = tokenList[0] 
            vector = np.fromstring(' '.join(tokenList[1:]), sep =' ')
            vectorMap[word]=vector
        pickle.dump(vectorMap,open(pickle_dump_path,"wb"))
    else:
        print "Using existing vector map"
        vectorMap = pickle.load(open(pickle_dump_path,"rb")) 
    return vectorMap

def loadRelationMap():
    relidFile=open("./data/processed/relationIDs.tsv","r")
    data = filter(None,relidFile.read().split('\n'))
    relationMap = {}
    for line in data:
        temp = line.split('\t')
        relationMap[temp[1]] = int(temp[0])
    return relationMap

def load_buckets(guidMap, relationMap, refresh=False):
    """
        return type : Dictionary object named buckets
        buckets keys : file no. example 100000
        buckets value : A list with the following mapping ->
            0th index : entity 1
            1st index : entity 2
            2nd index : relation id
            3rd index : list of pharases
            4th index : sentence count
    """
    pickle_dump_path = "./data/interim/buckets.p"
    sample_file_path = "./data/raw/kb_manual/trainPositive/"
    sample_low_count = 100000
    sample_high_count = 119465
    if refresh or not os.path.isfile(pickle_dump_path):
        print "creating bucket"
        buckets = {}
        for i in range(sample_low_count,sample_high_count):
            inputFile= open(sample_file_path + str(i) + ".pb","rb")
            rel=Document_pb2.Relation()
            rel.ParseFromString(inputFile.read())
            inputFile.close()
            bucket_content = []
            sentences = []
            try:
                entity_1 = guidMap[rel.sourceGuid]
            except KeyError:
                print "entity 1 Key Error"
            try:
                entity_2 = guidMap[rel.destGuid]
            except KeyError:
                print "entity 2 Key Error"
            bucket_content.append(entity_1)
            bucket_content.append(entity_2)
            try: 
                relation = relationMap[rel.relType]
            except KeyError:
                print "Relation Key Error"
            bucket_content.append(relation)
            phrases = []
            count = len(rel.mention)
            for mention in rel.mention:
                phrase1=re.match("(.*)"+entity_1.decode('utf-8')+"(.*)"+entity_2.decode('utf-8')+"(.*)",mention.sentence, re.UNICODE)
                phrase2=re.match("(.*)"+entity_2.decode('utf-8')+"(.*)"+entity_1.decode('utf-8')+"(.*)",mention.sentence, re.UNICODE)
                if phrase1:
                    phrase=phrase1.group(2)
                elif phrase2:
                    phrase=phrase2.group(2)
                else:
                    phrase = 'not-not-found'
                    print "Error in finding phrase for", entity_1, entity_2, mention.sentence, rel.sourceGuid, rel.destGuid
                phrases.append(phrase)
            bucket_content.append(phrases)
            bucket_content.append(count)
            buckets[i] = bucket_content
        pickle.dump(buckets, open(pickle_dump_path, "wb"))
    else:
        print "Using existing bucket"
        buckets = pickle.load(open(pickle_dump_path, "rb"))
    return buckets

def bucket_to_vectors(buckets, vectorMap, refresh=False, remove_duplicate = False):
    pickle_dump_path = "./data/interim/buckets_vectors.p"
    if refresh or not os.path.isfile(pickle_dump_path):
        print "creating bucket vector"
        N = 121867 # max total no. of sentences in this dataset

        # initialize vectors to max length N
        counter = 0
        entity_1_vectors = np.zeros((N,50), dtype=np.float32)
        entity_2_vectors = np.zeros((N,50), dtype=np.float32)
        phrase_vectors = np.zeros((N,50), dtype=np.float32)
        label_vector = np.zeros(N)
        phrases_list = []

        for key in buckets.keys():
            # discard phrases that contain a word not present in vectorMap using this flag
            error_while_vectorization = False
            data = buckets[key]

            entity_1 = data[0].split(' ')
            entity_2 = data[1].split(' ')
           
            # initialize entity vectors to zeros
            vector_e1 = np.zeros(50, dtype=np.float32)
            vector_e2 = np.zeros(50, dtype=np.float32)

            for word in entity_1:
                try:
                    vector_e1 += vectorMap[word.lower()]
                except KeyError:
                    print "entity 1", word.lower(), "not found in vectorMap"
                    error_while_vectorization = True        
            vector_e1 /= len(entity_1)

            for word in entity_2:
                try:
                    vector_e2 += vectorMap[word.lower()]
                except KeyError:
                    print "entity 2", word.lower(), "not found"
                    error_while_vectorization = True        
            vector_e2 /= len(entity_2)

            for phrase in data[3]:
                # initialize phrase vector to zero
                vector_phrase = np.zeros(50, dtype=np.float32)

                words = filter(None,phrase.split(' '))
                for word in words:
                    try:
                        vector_phrase += vectorMap[word.lower()]
                    except KeyError:
                        print "Phrase", word.lower(), "not found"
                        error_while_vectorization = True        
                vector_phrase /= len(data[3])
                if not error_while_vectorization:
                    entity_1_vectors[counter] = vector_e1
                    entity_2_vectors[counter] = vector_e2
                    phrase_vectors[counter] = vector_phrase
                    label_vector[counter] = data[2]
                    phrases_list.append(phrase)
                    counter += 1
        print counter

        # discard vectors indexes above counter count
        entity_1_vectors = entity_1_vectors[0:counter]
        entity_2_vectors = entity_2_vectors[0:counter]
        phrase_vectors = phrase_vectors[0:counter]
        label_vector = label_vector[0:counter]

        phrases_label = np.array(phrases_list, dtype=object)

        X_train = np.hstack((entity_1_vectors, entity_2_vectors, phrase_vectors))
        pickle.dump((X_train, label_vector, phrases_label), open(pickle_dump_path, "wb"))
    else:
        print "Using existing bucket vector"
        X_train, label_vector, phrases_label = pickle.load(open(pickle_dump_path, "rb"))
    return X_train, label_vector, phrases_label

if __name__ == "__main__":
    guidMap=loadGuidMap()
    vectorMap = loadVectorMap(refresh=False)
    relationMap = loadRelationMap()
    buckets = load_buckets(guidMap, relationMap, refresh=False)
    X_train, y_label, phrases_label = bucket_to_vectors(buckets, vectorMap, refresh=False)
   
    # extract samples for one particular relation type
    # id 2 = /people/person/place_of_birth
    sample_idx = y_label == 2
    X_sample = X_train[sample_idx]
    phrase_sample = phrases_label[sample_idx]


    # hierarchical clustering sample
    # source https://joernhees.de/blog/2015/08/26/scipy-hierarchical-clustering-and-dendrogram-tutorial/

    phrase_vectors = X_sample[:,50:100]
    Z = linkage(phrase_vectors, 'single', metric='cosine')

    plt.figure(figsize=(125, 50))
    plt.title('Hierarchical Clustering Dendrogram')
    plt.xlabel('phrase')
    plt.ylabel('distance')
    dendrogram(Z, 
            leaf_rotation=90.,  # rotates the x axis labels
            leaf_font_size=8.,  # font size for the x axis labels
            labels= phrase_sample
            )
    plt.show()

    #plt.savefig('phrase_plot.png')    
    # remove duplicates
    #X_train_red = np.ascontiguousarray(X_train).view(np.dtype((np.void, X_train.dtype.itemsize * X_train.shape[1])))
    #_, idx = np.unique(X_train_red, return_index=True)
    #X_train_red = X_train[idx]
    #y_label_red = y_label[idx]

    #count_list = []
    #for key in buckets.keys():
    #    count_list.append(buckets[key][-1])
    #print "My watch has ended"
                
