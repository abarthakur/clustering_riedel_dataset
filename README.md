# Clustering and Visualization of the Riedel dataset

This project processes the Riedel dataset introduced in [Riedel et al.(2010)](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.366.6512&rep=rep1&type=pdf), and performs clustering and visualization on it. The dataset can be downloaded from [here](http://iesl.cs.umass.edu/riedel/ecml/).    


## Datasets required
glove word vectors : Place or symbolic link glove.6B directory in data/external/
riedel dataset : Place or symbolic link riedel_ecml directory in data/external/ (extract all zipped files into riedel_ecml) 

The Riedel dataset is present in protobuf format. All relations are dumped into a single file, separated by a delimiter. Unfortunately, protobuf's python API does not support reading from delimited files. Hence, protobuf's java API is first used to separate the relations. Then the relations are converted into TSV format, from protobuf format. Both conversions can be done by running -

```bash clustering_riedel_dataset/src/java_to_python_protobuf/convert.bash```
```bash clustering_riedel_dataset/src/pb_to_single_tsv/convert.bash```

Each relation in the dataset consists of (loosely) the source and destination ids, and a list of mentions ( which are defined by the filename and the sentence no). These relations were extracted after annotation of the NYT corpus using Freebase( a knowledge base). Documents contain sentences, and sentences include a dependency tree ( generated using the MALT parser) and POS tags. 

For each mention of a relation, we extract -
1. The source and destination entities
2. The phrase in between the source and destination
3. The shortest path(of words) in the dependency tree between source and destination (``shortest_dp.py``)

We use these features (words),in various combinations, and convert them to vectors using GloVe word representation, and apply tSNE and hierarchial clustering to look for patterns in the data - especially for wrongly annotated sentences and sentences that violate the distance supervision assumption ( all sentences that mention two entities participating in a relation, express that relation).

The experiment can be replicated by running ``visualize.py``.
