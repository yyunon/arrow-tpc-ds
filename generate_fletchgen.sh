#!/bin/bash

#This is taken from https://github.com/abs-tudelft/fletcher/blob/develop/codegen/test/sodabeer/generate.sh and modified accordingly.
#-i input/Hobbiton.fbs input/Bywater.fbs input/Soda.fbs input/Beer.fbs 
source ./python/.env

fletchgen -r $DATASET_DIR/$TIME_PREFIX/ss_recordbatch.rb $DATASET_DIR/$TIME_PREFIX/s_recordbatch.rb $DATASET_DIR/$TIME_PREFIX/dt_recordbatch.rb $DATASET_DIR/$TIME_PREFIX/ca_recordbatch.rb $DATASET_DIR/$TIME_PREFIX/cd_recordbatch.rb  \
          -s tpc.srec \
          -t tpc_out.srec \
          -l vhdl \
          --sim
