#!/bin/bash

#This is taken from https://github.com/abs-tudelft/fletcher/blob/develop/codegen/test/sodabeer/generate.sh and modified accordingly.
#-i input/Hobbiton.fbs input/Bywater.fbs input/Soda.fbs input/Beer.fbs 
fletchgen -r $DATASET_DIR/2020_11_12_06_30/ss_recordbatch.rb $DATASET_DIR/2020_11_12_06_30/s_recordbatch.rb $DATASET_DIR/2020_11_12_06_30/dt_recordbatch.rb $DATASET_DIR/2020_11_12_06_30/ca_recordbatch.rb $DATASET_DIR/2020_11_12_06_30/cd_recordbatch.rb  \
          -s tpc.srec \
          -t tpc_out.srec \
          -l vhdl \
          --sim
