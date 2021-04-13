import os
import pyarrow as pa
from pyarrow import csv

# This model will hold the dataset and schema information.
 
class dataset:
    def __init__(self, c_prefix=None, metadata_information=None):
        #TODO Add many of the vars and logic in datasets to here
        self.database_name=None
        self.prefix= None
        self.recordbatch_name=None
        self.table=None
        self.data=[]
        self.txt_data=[]
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
        from os import path
        if path.exists(".env"):
            env_file = ".env"
        else:
            env=input('Could not find .env. Enter the .env directory pwd(No trailing backspace and use the same directory with fletchgen or generate_fletchgen): \n')
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
    def stream_to_txt_raw(mylist,filename):
        txt_file = open(filename,'w')
        for el in mylist:
            txt_file.write(str(el))
            txt_file.write(',')
        txt_file.write('\n')
        txt_file.close()


class store_sales(dataset):

    def __init__(self, columns=None, row_size = None, c_prefix=None,metadata_information=None,field_metadata=None,metadata_indexes=None):
        if metadata_information == None:
            assert(False)
        super().__init__(c_prefix, metadata_information)
        self.field_metadata = field_metadata
        self.metadata_information=metadata_information
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
        if metadata_indexes != None:
            print(f"Adding field metadata to indexes: ")
            for i in metadata_indexes:
                print(f"{self.ss_columns_index[i]}")
                self.ss_columns[self.ss_columns_index[i]] = self.ss_columns[self.ss_columns_index[i]].add_metadata(self.field_metadata)
        self.schema_vars = []
        for i in columns:
            self.schema_vars.append(self.ss_columns[self.ss_columns_index[i]])
        self.schema = pa.schema(self.schema_vars,metadata=self.metadata_information)
        #self.schema.add_metadata(self.metadata_information)
        # Field metadata such as epc, not always applicable
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
                    if row_size == None:
                        self.row_size = len(self.table.column(i).to_pylist())
                    temp_read = self.table.column(i).to_pylist()
                    self.txt_data.append(temp_read[0:row_size])
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

class store(dataset):

    def __init__(self, columns=None,row_size=None, c_prefix=None, metadata_information=None,field_metadata=None,metadata_indexes=None):
        if metadata_information == None:
            assert(false)
        super().__init__(c_prefix, metadata_information)
        self.field_metadata = field_metadata
        self.metadata_information=metadata_information
        # configure schema
        self.s_columns_index = [
             "s_store_sk",
             "s_store_id",
             "s_rec_start_date",
             "s_rec_end_date",
             "s_closed_date_sk",
             "s_store_name",
             "s_number_employees",
             "s_floor_space",
             "s_hours",
             "s_manager",
             "s_market_id",
             "s_geography_class",
             "s_market_desc",
             "s_market_manager",
             "s_division_id",
             "s_division_name",
             "s_company_id",
             "s_company_name",
             "s_street_number",
             "s_street_name",
             "s_street_type",
             "s_suite_number",
             "s_city",
             "s_county",
             "s_state",
             "s_zip",
             "s_country",
             "s_gmt_offset",
             "s_tax_precentage"
        ]
        self.s_columns = {
             "s_store_sk": pa.field('store_sk', pa.int64(), nullable=False),
             "s_store_id": pa.field('store_id', pa.utf8(), nullable=False),
             "s_rec_start_date": pa.field('rec_start_date', pa.utf8(), nullable=False),
             "s_rec_end_date": pa.field('rec_end_date', pa.utf8(), nullable=False),
             "s_closed_date_sk": pa.field('closed_date_sk', pa.int64(), nullable=False),
             "s_store_name": pa.field('store_name', pa.utf8(), nullable=False),
             "s_number_employees": pa.field('number_employees', pa.int64(), nullable=False),
             "s_floor_space": pa.field('floor_space', pa.int64(), nullable=False),
             "s_hours": pa.field('hours', pa.utf8(), nullable=False),
             "s_manager": pa.field('manager', pa.utf8(), nullable=False),
             "s_market_id": pa.field('market_id', pa.utf8(), nullable=False),
             "s_geography_class": pa.field('geography_class', pa.utf8(), nullable=False),
             "s_market_desc": pa.field('market_desc', pa.utf8(), nullable=False),
             "s_market_manager": pa.field('market_manager', pa.utf8(), nullable=False),
             "s_division_id": pa.field('division_id', pa.int64(), nullable=False),
             "s_division_name": pa.field('division_name', pa.utf8(), nullable=False),
             "s_company_id": pa.field('company_id', pa.int64(), nullable=False),
             "s_company_name": pa.field('company_name', pa.utf8(), nullable=False),
             "s_street_number": pa.field('street_number', pa.utf8(), nullable=False),
             "s_street_name": pa.field('street_name', pa.utf8(), nullable=False),
             "s_street_type": pa.field('street_type', pa.utf8(), nullable=False),
             "s_suite_number": pa.field('suite_number', pa.utf8(), nullable=False),
             "s_city": pa.field('city', pa.utf8(), nullable=False),
             "s_county": pa.field('county', pa.utf8(), nullable=False),
             "s_state": pa.field('state', pa.utf8(), nullable=False),
             "s_zip": pa.field('zip', pa.utf8(), nullable=False),
             "s_country": pa.field('country', pa.utf8(), nullable=False),
             "s_gmt_offset": pa.field('gmt_offset', pa.float64(), nullable=False),
             "s_tax_precentage": pa.field('tax_precentage', pa.float64(), nullable=False)
        }
        #TODO Merge multiple for loops, do this for all
        if metadata_indexes != None:
            print(f"Adding field metadata to indexes: ")
            for i in metadata_indexes:
                print(f"{self.s_columns_index[i]}")
                self.s_columns[self.s_columns_index[i]] = self.s_columns[self.s_columns_index[i]].add_metadata(self.field_metadata)
        self.schema_vars = []
        for i in columns:
            self.schema_vars.append(self.s_columns[self.s_columns_index[i]])
        self.schema = pa.schema(self.schema_vars,metadata=self.metadata_information)
        #self.schema.add_metadata()
        # field metadata such as epc, not always applicable
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
            print(f"Size of read table is: {len(self.table)} rows. You have selected to stream {row_size} number of rows.")
            for i,_ in enumerate(self.table):
                if i in columns: 
                    if row_size == None:
                        self.row_size = len(self.table.column(i).to_pylist())
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

    def __init__(self, columns=None,row_size=None, c_prefix=None,metadata_information=None,field_metadata=None,metadata_indexes=None):
        if metadata_information == None:
            assert(False)
        super().__init__(c_prefix, metadata_information)
        # configure schema
        self.metadata_information=metadata_information
        self.field_metadata = field_metadata
        self.dt_columns_index = [
            "d_date_sk",
            "d_date_id",
            "d_date",
            "d_month_seq",
            "d_week_seq",
            "d_quarter_seq",
            "d_year",
            "d_dow",
            "d_moy",
            "d_dom",
            "d_qoy",
            "d_fy_year",
            "d_fy_quarter_seq",
            "d_fy_week_seq",
            "d_day_name",
            "d_quarter_name",
            "d_holiday",
            "d_weekend",
            "d_following_holiday",
            "d_first_dom",
            "d_last_dom",
            "d_same_day_ly",
            "d_same_day_lq",
            "d_current_day",
            "d_current_week",
            "d_current_month",
            "d_current_quarter",
            "d_current_year"
        ]
        self.dt_columns = {
            "d_date_sk": pa.field('date_sk', pa.int64(), nullable=False),
            "d_date_id": pa.field('date_id', pa.utf8(), nullable=False),
            "d_date": pa.field('date', pa.utf8(), nullable=False),
            "d_month_seq": pa.field('month_seq', pa.int64(), nullable=False),
            "d_week_seq": pa.field('week_seq', pa.int64(), nullable=False),
            "d_quarter_seq": pa.field('quarter_seq', pa.int64(), nullable=False),
            "d_year":pa.field('year', pa.int64(), nullable=False),
            "d_dow": pa.field('dow', pa.int64(), nullable=False),
            "d_moy": pa.field('moy', pa.int64(), nullable=False),
            "d_dom": pa.field('dom', pa.int64(), nullable=False),
            "d_qoy": pa.field('qoy', pa.int64(), nullable=False),
            "d_fy_year": pa.field('fy_year', pa.int64(), nullable=False),
            "d_fy_quarter_seq": pa.field('fy_quarter_seq', pa.int64(), nullable=False),
            "d_fy_week_seq": pa.field('fy_week_seq', pa.int64(), nullable=False),
            "d_day_name": pa.field('day_name', pa.utf8(), nullable=False),
            "d_quarter_name": pa.field('quarter_name', pa.utf8(), nullable=False),
            "d_holiday": pa.field('holiday', pa.utf8(), nullable=False),
            "d_weekend": pa.field('weekend', pa.utf8(), nullable=False),
            "d_following_holiday": pa.field('following_holiday', pa.utf8(), nullable=False),
            "d_first_dom": pa.field('first_dom', pa.int64(), nullable=False),
            "d_last_dom": pa.field('last_dom', pa.int64(), nullable=False),
            "d_same_day_ly": pa.field('same_day_ly', pa.int64(), nullable=False),
            "d_same_day_lq": pa.field('same_day_lq', pa.int64(), nullable=False),
            "d_current_day": pa.field('current_day', pa.utf8(), nullable=False),
            "d_current_week": pa.field('current_week', pa.utf8(), nullable=False),
            "d_current_month": pa.field('current_month', pa.utf8(), nullable=False),
            "d_current_quarter": pa.field('current_quarter', pa.utf8(), nullable=False),
            "d_current_year": pa.field('current_year', pa.utf8(), nullable=False)
        }
        #TODO Merge multiple for loops, do this for all
        if metadata_indexes != None:
            print(f"Adding field metadata to indexes: ")
            for i in metadata_indexes:
                print(f"{self.dt_columns_index[i]}")
                self.dt_columns[self.dt_columns_index[i]] = self.dt_columns[self.dt_columns_index[i]].add_metadata(self.field_metadata)
        self.schema_vars = []
        for i in columns:
            self.schema_vars.append(self.dt_columns[self.dt_columns_index[i]])
        self.schema = pa.schema(self.schema_vars,metadata=self.metadata_information)
        #self.schema.add_metadata()
        # field metadata such as epc, not always applicable

        self.recordbatch_name="dt_recordbatch.rb"
        self.database_name = "dataset/date_dim.dat"
        #self.schema.add_metadata(self.metadata_information)
        # Field metadata such as epc, not always applicable
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
            print(f"Size of read table is: {len(self.table)} rows. You have selected to stream {row_size} number of rows.")
            for i,_ in enumerate(self.table):
                if i in columns: 
                    if row_size == None:
                        self.row_size = len(self.table.column(i).to_pylist())
                    temp_read = self.table.column(i).to_pylist()
                    self.txt_data.append(temp_read[0:row_size])
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

class customer_address(dataset):

    def __init__(self, columns=None,row_size = None, c_prefix=None,metadata_information=None,field_metadata=None,metadata_indexes=None):
        if metadata_information == None:
            assert(False)
        super().__init__(c_prefix, metadata_information)
        self.metadata_information=metadata_information
        self.field_metadata = field_metadata
        # configure schema
        self.ca_columns_index = [
            "ca_address_sk",
            "ca_address_id",
            "ca_street_number",
            "ca_street_name",
            "ca_street_type",
            "ca_suite_number",
            "ca_city",
            "ca_county",
            "ca_state",
            "ca_zip",
            "ca_country",
            "ca_gmt_offset",
            "ca_location_type"
        ]
        self.ca_columns = {
            "ca_address_sk":        pa.field('address_sk', pa.int64(), nullable=False),
            "ca_address_id":        pa.field('address_id', pa.utf8(), nullable=False),
            "ca_street_number":     pa.field('street_number', pa.utf8(), nullable=False),
            "ca_street_name":       pa.field('street_name', pa.utf8(), nullable=False),
            "ca_street_type":       pa.field('street_type', pa.utf8(), nullable=False),
            "ca_suite_number":      pa.field('suite_number', pa.utf8(), nullable=False),
            "ca_city":              pa.field('city', pa.utf8(), nullable=False),
            "ca_county":            pa.field('county', pa.utf8(), nullable=False),
            "ca_state":             pa.field('state', pa.utf8(), nullable=False),
            "ca_zip":               pa.field('zip', pa.utf8(), nullable=False),
            "ca_country":           pa.field('country', pa.utf8(), nullable=False),
            "ca_gmt_offset":        pa.field('gmt_offset', pa.float64(), nullable=False),
            "ca_location_type":     pa.field('location_type', pa.utf8(), nullable=False),
        }
        #TODO Merge multiple for loops, do this for all
        if metadata_indexes != None:
            print(f"Adding field metadata to indexes: ")
            for i in metadata_indexes:
                print(f"{self.ca_columns_index[i]}")
                self.ca_columns[self.ca_columns_index[i]] = self.ca_columns[self.ca_columns_index[i]].add_metadata(self.field_metadata)
        self.schema_vars = []
        for i in columns:
            self.schema_vars.append(self.ca_columns[self.ca_columns_index[i]])
        self.schema = pa.schema(self.schema_vars,metadata=self.metadata_information)
        #self.ca_state=self.ca_state.add_metadata(self.field_metadata)
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
            print(f"Size of read table is: {len(self.table)} rows. You have selected to stream {row_size} number of rows.")
            for i,_ in enumerate(self.table):
                if i in columns: 
                    if row_size == None:
                        self.row_size = len(self.table.column(i).to_pylist())
                    temp_read = self.table.column(i).to_pylist()
                    self.txt_data.append(temp_read[0:row_size])
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


class customer_demographics(dataset):

    def __init__(self, columns=None,row_size = None, c_prefix=None,metadata_information=None,field_metadata=None,metadata_indexes=None):
        #TODO implement metadata indexes
        if metadata_information == None:
            assert(False)
        super().__init__(c_prefix, metadata_information)
        self.field_metadata = field_metadata
        self.metadata_information=metadata_information
        # configure schema
        self.cd_columns_index = [
            "cd_demo_sk",
            "cd_gender",
            "cd_marital_status",
            "cd_education_status",
            "cd_purchase_estimate",
            "cd_credit_rating",
            "cd_dep_count",
            "cd_dep_employed_count",
            "cd_dep_college_count"
        ]
        self.cd_columns = {
            "cd_demo_sk":               pa.field('demo_sk', pa.int64(), nullable=False),
            "cd_gender":                pa.field('gender', pa.utf8(), nullable=False),
            "cd_marital_status":        pa.field('marital_status', pa.utf8(), nullable=False),
            "cd_education_status":      pa.field('education_status', pa.utf8(), nullable=False),
            "cd_purchase_estimate":     pa.field('purchase_estimate', pa.int64(), nullable=False),
            "cd_credit_rating":         pa.field('credit_rating', pa.utf8(), nullable=False),
            "cd_dep_count":             pa.field('dep_count', pa.int64(), nullable=False),
            "cd_dep_employed_count":    pa.field('dep_employed_count', pa.int64(), nullable=False),
            "cd_dep_college_count":     pa.field('dep_college_count', pa.int64(), nullable=False)
        }
        #TODO Merge multiple for loops, do this for all
        if metadata_indexes != None:
            print(f"Adding field metadata to indexes: ")
            for i in metadata_indexes:
                print(f"{self.cd_columns_index[i]}")
                self.cd_columns[self.cd_columns_index[i]] = self.cd_columns[self.cd_columns_index[i]].add_metadata(self.field_metadata)
        self.schema_vars = []
        for i in columns:
            self.schema_vars.append(self.cd_columns[self.cd_columns_index[i]])
        self.schema = pa.schema(self.schema_vars,metadata=self.metadata_information)
        #self.cd_marital_status=self.cd_marital_status.add_metadata(self.field_metadata)
        #self.cd_education_status=self.cd_education_status.add_metadata(self.field_metadata)
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
            print(f"Size of read table is: {len(self.table)} rows. You have selected to stream {row_size} number of rows.")
            for i,_ in enumerate(self.table):
                if i in columns: 
                    if row_size == None:
                        self.row_size = len(self.table.column(i).to_pylist())
                    temp_read = self.table.column(i).to_pylist()
                    self.txt_data.append(temp_read[0:row_size])
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

# TPCH
class line_item(dataset):

    def __init__(self, columns=None, row_size = None, c_prefix=None,metadata_information=None,field_metadata=None,metadata_indexes=None, convert_to_fixed=False, convert_date_to_int=False, custom_date_encoding=False):
        if metadata_information == None:
            assert(False)
        super().__init__(c_prefix, metadata_information)
        self.field_metadata = field_metadata
        self.metadata_information=metadata_information
        self.l_columns_index = [
            'l_orderkey',
            'l_partkey',
            'l_suppkey',
            'l_linenumber',
            'l_quantity',
            'l_extendedprice',
            'l_discount',
            'l_tax',
            'l_returnflag',
            'l_linestatus',
            'l_shipdate',
            'l_commitdate',
            'l_receiptdate',
            'l_shipinstruct',
            'l_shipmode',
            'l_comment',
        ]
        self.l_columns = {
            'l_orderkey'       : pa.field('orderkey', pa.int64(), nullable=False),
            'l_partkey'       : pa.field('partkey', pa.int64(), nullable=False),
            'l_suppkey'       : pa.field('suppkey', pa.int64(), nullable=False),
            'l_linenumber'       : pa.field('linenumber', pa.int64(), nullable=False),
            'l_quantity'   : pa.field('quantity', pa.float64(), nullable=False), #
            'l_extendedprice'           : pa.field('extendedprice', pa.float64(), nullable=False), #
            'l_discount'           : pa.field('discount', pa.float64(), nullable=False), #
            'l_tax'            : pa.field('tax', pa.int64(), nullable=False), #
            'l_returnflag'           : pa.field('returnflag', pa.utf8(), nullable=False),
            'l_linestatus'           : pa.field('linestatus', pa.utf8(), nullable=False),
            'l_shipdate'      : pa.field('shipdate', pa.date32(), nullable=False),
            'l_commitdate'           : pa.field('commitdate', pa.date64(), nullable=False),
            'l_receiptdate'     : pa.field('receiptdate', pa.date64(), nullable=False),
            'l_shipinstruct'         : pa.field('shipinstruct', pa.utf8(), nullable=False),
            'l_shipmode'        : pa.field('shipmode', pa.utf8(), nullable=False),
            'l_comment'   : pa.field('comment', pa.utf8(), nullable=False),
        }
        self.pq_table=None
        if metadata_indexes != None:
            print(f"Adding field metadata to indexes: ")
            for i in metadata_indexes:
                print(f"{self.l_columns_index[i]}")
                self.l_columns[self.l_columns_index[i]] = self.l_columns[self.l_columns_index[i]].with_metadata(self.field_metadata)
        self.schema_vars = []
        for i in columns:
            self.schema_vars.append(self.l_columns[self.l_columns_index[i]])
        self.schema = pa.schema(self.schema_vars,metadata=self.metadata_information)
        #self.schema.add_metadata(self.metadata_information)
        # Field metadata such as epc, not always applicable
        # output file names
        self.recordbatch_name="l_recordbatch.rb"
        self.database_name = "dataset/lineitem.dat"
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
                    if row_size == None:
                        self.row_size = len(self.table.column(i).to_pylist())
                    temp_read = self.table.column(i).to_pylist()
                    if convert_date_to_int == True and (i == 10 or i ==11 or i==12):
                        dates= []
                        for i in range(len(temp_read)):
                            if custom_date_encoding == True:
                                def to_integer(dt_time):
                                    return 10000*dt_time.year + 100*dt_time.month + dt_time.day
                                dates.append(to_integer(temp_read[i]))
                            else:
                                import datetime
                                import numpy as np
                                epoch = datetime.datetime(1970,1,1)
                                def unix_time_millis(dt):
                                    return np.int32((dt - epoch).days)
                                dates.append(unix_time_millis(temp_read[i]))
                        temp_read = dates
                    if (i==4 or i==5 or i==6 or i==7):
                        temp_read = [float(i) for i in temp_read]
                    if convert_to_fixed == True and (i==4 or i==5 or i==6 or i==7):
                        temp_read = [float(i) for i in temp_read]
                        def to_fixed(f,e):
                            a = f* (2**e)
                            b = int(round(a))
                            if a < 0:
                                # next three lines turns b into it's 2's complement.
                                b = abs(b)
                                b = ~b
                                b = b + 1
                            return b
                        fixed_pts=[]
                        for i in range(len(temp_read)):
                            y_fxp = to_fixed(temp_read[i], 18)
                            fixed_pts.append(y_fxp)
                        temp_read = fixed_pts
                    self.txt_data.append(temp_read[0:row_size])
                    self.data.append(pa.array(temp_read[0:row_size]))

    def stream_to_parquet(self):
        import pyarrow.parquet as pq
        print(f"Streaming to file parquet.....")
        self.pq_table = pa.Table.from_arrays(self.data, schema= self.schema)
        # Create an Arrow RecordBatchFileWriter.
        self.writer = pq.write_table(self.pq_table, self.dir_name + "/lineitem.parquet")

    def stream_to_file(self):
        print(f"Streaming to file {self.recordbatch_name}.....")
        print(self.data)
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


