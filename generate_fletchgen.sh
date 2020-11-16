#!/bin/bash

#This is taken from https://github.com/abs-tudelft/fletcher/blob/develop/codegen/test/sodabeer/generate.sh and modified accordingly.
#-i input/Hobbiton.fbs input/Bywater.fbs input/Soda.fbs input/Beer.fbs 
#rm -rf ./build
source ./python/python/.env

#mkdir build && cd build
fletchgen -n Join -r $DATASET_DIR/$TIME_PREFIX/ss_recordbatch.rb $DATASET_DIR/$TIME_PREFIX/s_recordbatch.rb \
          -s memory.srec \
          #-t tpc_out.srec \
          -l vhdl \
          --sim \
          --axi
