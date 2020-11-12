import pyarrow as pa
from pyarrow import csv

# This model will hold the dataset and schema information.
 
class dataset:
    def __init__(self, c_prefix=None):
        self.prefix= None
        self.recordbatch_name=None
        self.table=None
        self.data=None
        self.metadata = None
        self.schema=None
        self.opt = csv.ParseOptions()
        if c_prefix == None:
            import os
            for k,v in os.environ.items():
                if k =='DATASET_DIR':
                    self.prefix = v
            print(f"Using auto environment variable: {self.prefix} ")
        else:
            #TODO: Write later on
            assert(False)
    @property
    def options(self):
        return self.opt
    @options.setter
    def options(self, d="|"):
        self.opt.delimeter(d)
    def stream_to_file(self):
        pass

class store_sales(dataset):
    self.database_name = "store_sales.dat"
    self.ss_type = ['int64','int64','int64','int64','int64','float64','float64']
    self.ss_sold_date_sk = pa.field('ss_sold_date_sk', pa.int64(), nullable=False)
    self.ss_cdemo_sk= pa.field('ss_cdemo_sk', pa.int64(), nullable=False)
    self.ss_addr_sk= pa.field('ss_addr_sk', pa.int64(), nullable=False)
    self.ss_store_sk= pa.field('ss_store_sk', pa.int64(), nullable=False)
    self.ss_quantity= pa.field('ss_quantity', pa.int64(), nullable=False)
    self.ss_sales_price=pa.field('ss_sales_price', pa.float64(), nullable=False)
    self.ss_net_profit=pa.field('ss_net_profit', pa.float64(), nullable=False)
    def __init__(self,columns=None,metadata_information=None):
        if metadata_information == None:
            assert(False)
        super().__init__()
        self.table = None
        if columns == None:
            print("Reading all columns...")
            self.table = csv.read_csv(self.prefix + self.database_name,self.opt)
            for i,_ in enumerate(table):
                if i in columns: 
                    self.data.append(pa.array(table.column(i).to_pylist()))
        else: 
            print("Reading all columns : {columns}")
            self.table = csv.read_csv(self.prefix + self.database_name,self.opt)
            for i,_ in enumerate(table):
                if i in columns: 
                    self.data.append(pa.array(table.column(i).to_pylist()))
    def stream_to_file(self):
        recordbatch = pa.RecordBatch.from_arrays(self.data, schema=schema)

        # Create an Arrow RecordBatchFileWriter.
        writer = pa.RecordBatchFileWriter(self.prefix + "/" + recordbatch_name, schema)

        # Write the RecordBatch.
        writer.write(recordbatch)

        # Close the writer.
        writer.close()



