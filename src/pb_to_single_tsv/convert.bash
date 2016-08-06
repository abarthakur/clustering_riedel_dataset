#!/bin/bash
convert () {
  python pb_to_tsv.py $1 $2
}

if [ -z "$1"  ]
  then
    echo "No argument supplied"
    echo "./convert.bash 1 means convert data"
    echo "./convert.bash 0 means delete converted data"
    exit 1
fi

if [ $1 -eq 0 ]
then
  echo "deleting converted data"
  rm -rf ../../data/external/riedel_ecml/kb_manual/trainPositive.tsv
  rm -rf ../../data/external/riedel_ecml/kb_manual/trainNegative.tsv

  rm -rf ../../data/external/riedel_ecml/heldout_relations/trainPositive.tsv
  rm -rf ../../data/external/riedel_ecml/heldout_relations/trainNegative.tsv
  rm -rf ../../data/external/riedel_ecml/heldout_relations/testPositive.tsv
  rm -rf ../../data/external/riedel_ecml/heldout_relations/testNegative.tsv
else
  echo "converting data"
  #convert all files
  convert ../../data/external/riedel_ecml/kb_manual/trainPositive/ ../../data/external/riedel_ecml/filtered-freebase-simple-topic-dump-3cols.tsv 
  convert ../../data/external/riedel_ecml/kb_manual/trainNegative/ ../../data/external/riedel_ecml/filtered-freebase-simple-topic-dump-3cols.tsv

  convert ../../data/external/riedel_ecml/heldout_relations/trainPositive/ ../../data/external/riedel_ecml/filtered-freebase-simple-topic-dump-3cols.tsv
  convert ../../data/external/riedel_ecml/heldout_relations/trainNegative/ ../../data/external/riedel_ecml/filtered-freebase-simple-topic-dump-3cols.tsv
  convert ../../data/external/riedel_ecml/heldout_relations/testPositive/ ../../data/external/riedel_ecml/filtered-freebase-simple-topic-dump-3cols.tsv
  convert ../../data/external/riedel_ecml/heldout_relations/testNegative/ ../../data/external/riedel_ecml/filtered-freebase-simple-topic-dump-3cols.tsv
fi

