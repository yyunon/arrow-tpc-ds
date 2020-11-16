import os
import pyarrow as pa
from pyarrow import csv

# This model will hold the dataset and schema information.
 
class dataset:
    def __init__(self, c_prefix=None, metadata_information=None):
        self.database_name=None
        self.prefix= None
        self.recordbatch_name=None
        self.table=None
        self.data=[]
        self.schema=None
        self.recordbatch=None
        self.writer=None
        self.metadata_information=None
        self.metadata = metadata_information
        self.opt = csv.ParseOptions()
        if c_prefix == None:
            for k,v in os.environ.items():
                if k =='DATASET_DIR':
                    self.prefix = v + "/"
            print(f"Using auto environment variable: {self.prefix} ")
        else:
            #TODO: Write later on
            assert(False)
        import time
        #self.timestamp= time.strftime("%Y%m%d-%H%M%S")
        self.timestamp= time.strftime("%Y_%m_%d_%H_%M")
        self.dir_name = self.prefix + self.timestamp
        if not os.path.isdir(self.dir_name):
            os.mkdir(self.dir_name)
        #Create env var. to be read by fletchgen
        env=raw_input("Enter the .env directory pwd: ")
        with open(env, "w") as f:
            f.write('export TIME_PREFIX="'+str(self.timestamp) + '"')

    @property
    def options(self):
        return self.opt

    @options.setter
    def options(self, d='|'):
        self.opt.delimiter = d

class store_sales(dataset):

    def __init__(self, columns=None, row_size = None, c_prefix=None,metadata_information=None,field_metadata=None,metadata_indexes=None):
        if metadata_information == None or metadata_indexes==None:
            assert(False)
        super().__init__(c_prefix, metadata_information)
        self.ss_columns_index = [
            'ss_sold_date_sk',
            'ss_sold_time_sk',     
            'ss_sold_item_sk',    
            'ss_sold_customer_sk',
            'ss_cdemo_sk',
            'ss_hdemo_sk',
            'ss_addr_sk',
            'ss_store_sk',
            'ss_promo_sk',
            'ss_ticket_number',
            'ss_quantity',
            'ss_wholesale_cost',
            'ss_list_price',
            'ss_sales_price',
            'ss_ext_discount_amt',
            'ss_ext_sales_price',
            'ss_ext_wholesale_cost',
            'ss_ext_list_price',
            'ss_ext_tax',
            'ss_coupon_amt',
            'ss_net_paid',
            'ss_net_paid_inc_tax',
            'ss_net_profit'
        ]
        self.ss_columns = {
            'ss_sold_date_sk'       : pa.field('sold_date_sk', pa.int64(), nullable=False),
            'ss_sold_time_sk'       : pa.field('sold_time_sk', pa.int64(), nullable=False),
            'ss_sold_item_sk'       : pa.field('sold_item_sk', pa.int64(), nullable=False),
            'ss_sold_customer_sk'   : pa.field('sold_customer_sk', pa.int64(), nullable=False),
            'ss_cdemo_sk'           : pa.field('cdemo_sk', pa.int64(), nullable=False),
            'ss_hdemo_sk'           : pa.field('hdemo_sk', pa.int64(), nullable=False),
            'ss_addr_sk'            : pa.field('addr_sk', pa.int64(), nullable=False),
            'ss_store_sk'           : pa.field('store_sk', pa.int64(), nullable=False),
            'ss_promo_sk'           : pa.field('promo_sk', pa.int64(), nullable=False),
            'ss_ticket_number'      : pa.field('ticket_number', pa.int64(), nullable=False),
            'ss_quantity'           : pa.field('quantity', pa.int64(), nullable=False),
            'ss_wholesale_cost'     : pa.field('wholesale_cost', pa.float64(), nullable=False),
            'ss_list_price'         : pa.field('list_price', pa.float64(), nullable=False),
            'ss_sales_price'        : pa.field('sales_price', pa.float64(), nullable=False),
            'ss_ext_discount_amt'   : pa.field('ext_discount_amt', pa.float64(), nullable=False),
            'ss_ext_sales_price'    : pa.field('ext_sales_price', pa.float64(), nullable=False),
            'ss_ext_wholesale_cost' : pa.field('ext_wholesale_cost', pa.float64(), nullable=False),
            'ss_ext_list_price'     : pa.field('ext_list_price', pa.float64(), nullable=False),
            'ss_ext_tax'            : pa.field('ext_tax', pa.float64(), nullable=False),
            'ss_coupon_amt'         : pa.field('coupon_amt', pa.float64(), nullable=False),
            'ss_net_paid'           : pa.field('net_paid', pa.float64(), nullable=False),
            'ss_net_paid_inc_tax'   : pa.field('net_paid_inc_tax', pa.float64(), nullable=False),
            'ss_net_profit'         : pa.field('net_profit', pa.float64(), nullable=False)
        }
        self.metadata_information=metadata_information
        self.schema_vars = []
        for i in columns:
            self.schema_vars.append(self.ss_columns_index[i])
        self.schema = pa.schema(self.schema_vars,metadata=self.metadata_information)
        #self.schema.add_metadata(self.metadata_information)
        # Field metadata such as epc, not always applicable
        self.field_metadata = field_metadata
        # output file names
        self.recordbatch_name="ss_recordbatch.rb"
        self.database_name = "dataset/store_sales.dat"
        self.options="|"
        if columns == None:
            print("Reading all columns...")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            for i,_ in enumerate(self.table):
                if i in columns: 
                    self.data.append(pa.array(self.table.column(i).to_pylist()))
        else: 
            print(f"Reading some columns : {columns}")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            print(f"Size of read table is: {len(self.table)} rows. You have selected to stream {row_size} number of rows.")
            for i,_ in enumerate(self.table):
                if i in columns: 
                    temp_read = self.table.column(i).to_pylist()
                    self.data.append(pa.array(temp_read[0:row_size]))

    def stream_to_file(self):
        print(f"Streaming to file {self.recordbatch_name}.....")
        self.recordbatch = pa.RecordBatch.from_arrays(self.data, schema=self.schema)

        # Create an Arrow RecordBatchFileWriter.
        self.writer = pa.RecordBatchFileWriter(self.dir_name + "/" + self.recordbatch_name, self.schema)

        # Write the RecordBatch.
        self.writer.write(self.recordbatch)

        # Close the writer.
        self.writer.close()

    def print_col(self,column):
        assert(self.data != None)
        print(f"Column: {column}, Row:{self.data[column]} ")


class date_dim(dataset):

    def __init__(self, columns=None, c_prefix=None,metadata_information=None,field_metadata=None,metadata_indexes=None):
        if metadata_information == None or metadata_indexes==None:
            assert(False)
        super().__init__(c_prefix, metadata_information)

        self.recordbatch_name="dt_recordbatch.rb"
        self.database_name = "dataset/date_dim.dat"
        self.type = ['int64','int64']
        self.d_date_sk=pa.field('date_sk',pa.int64(), nullable=False)
        self.d_year=pa.field('year', pa.int64(),nullable=False)

        self.metadata_information=metadata_information
        # Configure schema
        #TODO : make this general
        self.schema = pa.schema([self.d_date_sk,
                                 self.d_year],metadata=self.metadata_information)
        #self.schema.add_metadata(self.metadata_information)
        # Field metadata such as epc, not always applicable
        self.field_metadata = field_metadata
        # output file names
        self.options="|"
        if columns == None:
            print("Reading all columns...")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            for i,_ in enumerate(self.table):
                if i in columns: 
                    self.data.append(pa.array(self.table.column(i).to_pylist()))
        else: 
            print(f"Reading some columns : {columns}")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            print(f"Size of table is: {len(self.table)}")
            for i,_ in enumerate(self.table):
                if i in columns: 
                    self.data.append(pa.array(self.table.column(i).to_pylist()))
        self.print_col(0)

    def stream_to_file(self):
        print(f"Streaming to file {self.recordbatch_name}.....")
        self.recordbatch = pa.RecordBatch.from_arrays(self.data, schema=self.schema)

        # Create an Arrow RecordBatchFileWriter.
        self.writer = pa.RecordBatchFileWriter(self.dir_name + "/" + self.recordbatch_name, self.schema)

        # Write the RecordBatch.
        self.writer.write(self.recordbatch)

        # Close the writer.
        self.writer.close()

    def print_col(self,column):
        assert(self.data != None)
        print(f"Column: {column}, Row:{self.data[column]} ")

class customer_address(dataset):

    def __init__(self, columns=None, c_prefix=None,metadata_information=None,field_metadata=None,metadata_indexes=None):
        if metadata_information == None or metadata_indexes==None:
            assert(False)
        super().__init__(c_prefix, metadata_information)
        self.field_metadata = field_metadata
        self.type=['int64','string','string']
        self.ca_address_sk=pa.field('address_sk', pa.int64(), nullable=False)
        self.ca_state=pa.field('state', pa.utf8(), nullable=False)
        self.ca_country=pa.field('country', pa.utf8(), nullable=False)
        self.metadata_information=metadata_information
        # Configure schema
        #TODO : make this general
        self.schema = pa.schema([self.ca_address_sk,
                                 self.ca_state,
                                 self.ca_country],metadata=self.metadata_information)
        #self.schema.add_metadata(metadata_information)
        self.ca_country=self.ca_country.add_metadata(self.field_metadata)
        self.ca_state=self.ca_state.add_metadata(self.field_metadata)
        # Field metadata such as epc, not always applicable
        # output file names
        self.recordbatch_name="ca_recordbatch.rb"
        self.database_name = "dataset/customer_address.dat"
        self.options="|"
        if columns == None:
            print("Reading all columns...")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            for i,_ in enumerate(self.table):
                if i in columns: 
                    self.data.append(pa.array(self.table.column(i).to_pylist()))
        else: 
            print(f"Reading some columns : {columns}")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            print(f"Size of table is: {len(self.table)}")
            for i,_ in enumerate(self.table):
                if i in columns: 
                    self.data.append(pa.array(self.table.column(i).to_pylist()))
        self.print_col(0)

    def stream_to_file(self):
        print(f"Streaming to file {self.recordbatch_name}.....")
        self.recordbatch = pa.RecordBatch.from_arrays(self.data, schema=self.schema)

        # Create an Arrow RecordBatchFileWriter.
        self.writer = pa.RecordBatchFileWriter(self.dir_name + "/" + self.recordbatch_name, self.schema)

        # Write the RecordBatch.
        self.writer.write(self.recordbatch)

        # Close the writer.
        self.writer.close()

    def print_col(self,column):
        assert(self.data != None)
        print(f"Column: {column}, Row:{self.data[column]} ")


class customer_demographics(dataset):

    def __init__(self, columns=None, c_prefix=None,metadata_information=None,field_metadata=None,metadata_indexes=None):
        #TODO implement metadata indexes
        if metadata_information == None or metadata_indexes==None:
            assert(False)
        super().__init__(c_prefix, metadata_information)
        self.field_metadata = field_metadata
        self.type = ['int64','string', 'string']
        self.cd_demo_sk=pa.field('demo_sk', pa.int64(), nullable=False)
        self.cd_marital_status=pa.field('marital_status', pa.utf8(),nullable=False)
        self.cd_education_status=pa.field('education_status', pa.utf8(), nullable= False)
        self.metadata_information=metadata_information
        # Configure schema
        #TODO : make this general
        self.schema = pa.schema([self.cd_demo_sk,
                                 self.cd_marital_status,
                                 self.cd_education_status],metadata=self.metadata_information)
        self.cd_marital_status=self.cd_marital_status.add_metadata(self.field_metadata)
        self.cd_education_status=self.cd_education_status.add_metadata(self.field_metadata)
        #self.schema.add_metadata(metadata_information)
        # Field metadata such as epc, not always applicable
        # output file names
        self.recordbatch_name="cd_recordbatch.rb"
        self.database_name = "dataset/customer_demographics.dat"
        self.options="|"
        if columns == None:
            print("Reading all columns...")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            for i,_ in enumerate(self.table):
                if i in columns: 
                    self.data.append(pa.array(self.table.column(i).to_pylist()))
        else: 
            print(f"Reading some columns : {columns}")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            print(f"Size of table is: {len(self.table)}")
            for i,_ in enumerate(self.table):
                if i in columns: 
                    self.data.append(pa.array(self.table.column(i).to_pylist()))
        self.print_col(0)

    def stream_to_file(self):
        print(f"Streaming to file {self.recordbatch_name}.....")
        self.recordbatch = pa.RecordBatch.from_arrays(self.data, schema=self.schema)

        # Create an Arrow RecordBatchFileWriter.
        self.writer = pa.RecordBatchFileWriter(self.dir_name + "/" + self.recordbatch_name, self.schema)

        # Write the RecordBatch.
        self.writer.write(self.recordbatch)

        # Close the writer.
        self.writer.close()

    def print_col(self,column):
        assert(self.data != None)
        print(f"Column: {column}, Row:{self.data[column]} ")

class store(dataset):

    def __init__(self, columns=None, c_prefix=None, metadata_information=None,field_metadata=None,metadata_indexes=None):
        if metadata_information == None or metadata_indexes==None:
            assert(false)
        super().__init__(c_prefix, metadata_information)
        self.type = ['int64']
        self.s_store_sk=pa.field('store_sk', pa.int64(), nullable=False)
        # configure schema
        self.metadata_information=metadata_information
        #todo : make this general
        self.schema = pa.schema([self.s_store_sk],metadata=self.metadata_information)
        #self.schema.add_metadata()
        # field metadata such as epc, not always applicable
        self.field_metadata = field_metadata
        # output file names
        self.recordbatch_name="s_recordbatch.rb"
        self.database_name = "dataset/store.dat"
        self.options="|"
        if columns == None:
            print("reading all columns...")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            for i,_ in enumerate(self.table):
                if i in columns: 
                    self.data.append(pa.array(self.table.column(i).to_pylist()))
        else: 
            print(f"reading some columns : {columns}")
            self.table = csv.read_csv(self.prefix + self.database_name,parse_options=self.opt)
            print(f"size of table is: {len(self.table)}")
            for i,_ in enumerate(self.table):
                if i in columns: 
                    self.data.append(pa.array(self.table.column(i).to_pylist()))
        self.print_col(0)

    def stream_to_file(self):
        print(f"Streaming to file {self.recordbatch_name}.....")
        self.recordbatch = pa.RecordBatch.from_arrays(self.data, schema=self.schema)

        # Create an Arrow RecordBatchFileWriter.
        self.writer = pa.RecordBatchFileWriter(self.dir_name + "/" + self.recordbatch_name, self.schema)

        # Write the RecordBatch.
        self.writer.write(self.recordbatch)

        # Close the writer.
        self.writer.close()

    def print_col(self,column):
        assert(self.data != None)
        print(f"Column: {column}, Row:{self.data[column]} ")

