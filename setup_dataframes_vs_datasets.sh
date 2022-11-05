#!/bin/bash

DATASET="https://figshare.com/ndownloader/files/38035083"
DATASET_FOLDER="./data/flight_delay"
FILENAME="$DATASET_FOLDER/dataset.zip"


create_dataset_path() {
  mkdir -p $DATASET_FOLDER
}

get_the_data() {
    wget  $DATASET -O $FILENAME
    unzip $FILENAME -d $DATASET_FOLDER
    rm $FILENAME
}


create_dataset_path
get_the_data