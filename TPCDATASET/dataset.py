import os
import pyarrow as pa
from pyarrow import csv

# This model will hold the dataset and schema information.


class Dataset:
    def __init__(self, c_prefix=None, metadata_information=None):
        # TODO Add many of the vars and logic in datasets to here
        self.database_name = None
        self.prefix = None
        self.recordbatch_name = None
        self.table = None
        self.data = []
        self.txt_data = []
        self.schema = None
        self.recordbatch = None
        self.writer = None
        self.metadata_information = None
        self.metadata = metadata_information
        self.opt = csv.ParseOptions()
        if c_prefix == None:
            for k, v in os.environ.items():
                if k == 'DATASET_DIR':
                    self.prefix = v + "/"
            print(f"Using auto environment variable: {self.prefix} ")
        else:
            # TODO: Write later on
            assert(False)
        import time
        #self.timestamp= time.strftime("%Y%m%d-%H%M%S")
        self.timestamp = time.strftime("%Y_%m_%d_%H_%M")
        self.dir_name = self.prefix + self.timestamp
        if not os.path.isdir(self.dir_name):
            os.mkdir(self.dir_name)
        # Create env var. to be read by fletchgen
        from os import path
        if path.exists(".env"):
            env_file = ".env"
        else:
            env = input(
                'Could not find .env. Enter the .env directory pwd(No trailing backspace and use the same directory with fletchgen or generate_fletchgen): \n')
            env_file = env + "/.env"

        with open(env_file, "w") as f:
            f.write('export TIME_PREFIX="'+str(self.timestamp) + '"')

    def py_list(self):
        return self.txt_data

    @property
    def options(self):
        return self.opt

    @options.setter
    def options(self, d='|'):
        self.opt.delimiter = d

    @staticmethod
    def stream_to_txt_raw(mylist, filename):
        txt_file = open(filename, 'w')
        for el in mylist:
            txt_file.write(str(el))
            txt_file.write(',')
        txt_file.write('\n')
        txt_file.close()
