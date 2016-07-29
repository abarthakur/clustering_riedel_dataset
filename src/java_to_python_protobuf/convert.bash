#!/usr/bin/bash
convert () {
  FILE=$1
  OUT=${FILE::-3}"/"
  mkdir -p $OUT
  java -cp ":./protobuf-java-2.6.1.jar" RelationReader $FILE $OUT
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
  rm -rf ../../data/external/riedel_ecml/kb_manual/trainPositive
  rm -rf ../../data/external/riedel_ecml/kb_manual/trainNegative

  rm -rf ../../data/external/riedel_ecml/heldout_relations/trainPositive 
  rm -rf ../../data/external/riedel_ecml/heldout_relations/trainNegative
  rm -rf ../../data/external/riedel_ecml/heldout_relations/testPositive
  rm -rf ../../data/external/riedel_ecml/heldout_relations/testNegative
else
  echo "converting data"
  javac -cp ":./protobuf-java-2.6.1.jar" RelationReader.java 
  #convert all files
  convert ../../data/external/riedel_ecml/kb_manual/trainPositive.pb
  convert ../../data/external/riedel_ecml/kb_manual/trainNegative.pb

  convert ../../data/external/riedel_ecml/heldout_relations/trainPositive.pb
  convert ../../data/external/riedel_ecml/heldout_relations/trainNegative.pb
  convert ../../data/external/riedel_ecml/heldout_relations/testPositive.pb
  convert ../../data/external/riedel_ecml/heldout_relations/testNegative.pb
fi

