import Document_pb2
import sys
import os
import codecs
import unicodecsv

def create_guid_dict(file_name):
    '''
    Create a dictionary with guids as key and entity name as value
    '''
    tsvin = unicodecsv.reader(codecs.open(file_name, 'r'), delimiter='\t')
    guid_dict = {}
    for row in tsvin:
        guid_dict[row[0]] = row[1]
    return guid_dict


def base32(num, numerals="0123456789bcdfghjklmnpqrstvwxyz_"):
    '''ref: http://code.activestate.com/recipes/65212/'''
    return ((num == 0) and numerals[0]) or (base32(num // 32, numerals).lstrip(numerals[0]) + numerals[num % 32])


def guid_to_mid(guid):
    '''
    Convert guid to mid. 
    Conversion rules mentioned at http://wiki.freebase.com/wiki/Guid
    '''
    guid = guid.split('/')[-1]
    # remove 9202a8c04000641f8
    guid = guid[17:]
    i = int(guid,16)
    mid = 'm.0' + str(base32(i))
    return mid

input_dir = sys.argv[1]
file_list = os.listdir(input_dir)

out_dir = os.sep.join(input_dir.split(os.sep)[:-2]) + os.sep
out_file = input_dir.split(os.sep)[-2] + '.tsv'
f_write = unicodecsv.writer(codecs.open(out_dir + out_file, 'w'), delimiter='\t')

print("Starting converting files in ", input_dir)
guid_mapping_file = sys.argv[2]
guid_dict = create_guid_dict(guid_mapping_file)

for file_name in file_list:
    rel = Document_pb2.Relation()
    f = open(input_dir + file_name, 'r')
    rel.ParseFromString(f.read())
    sourceId = rel.sourceGuid
    destId = rel.destGuid
    e1_name = guid_dict[sourceId]
    e2_name = guid_dict[destId]
    relation = rel.relType
    sourceId = guid_to_mid(sourceId)
    destId = guid_to_mid(destId)
    for mention in rel.mention:
        sentence = mention.sentence
        f_write.writerow([sourceId, destId, e1_name.lower(), e2_name.lower(), relation, sentence.lower()])
    f.close()
