import pyarrow as pa
from TPCDATASET import dataset

data = dataset()

#this example is for RecordBatch ,RB, output of query 48.
DEBUG = 1

#Store Sales table - RB
ss = [0,4,6,7,10,13,22]
ss_type = ['int64','int64','int64','int64','int64','float64','float64']
ss_sold_date_sk = pa.field('ss_sold_date_sk', pa.int64(), nullable=False)
ss_cdemo_sk= pa.field('ss_cdemo_sk', pa.int64(), nullable=False)
ss_addr_sk= pa.field('ss_addr_sk', pa.int64(), nullable=False)
ss_store_sk= pa.field('ss_store_sk', pa.int64(), nullable=False)
ss_quantity= pa.field('ss_quantity', pa.int64(), nullable=False)
ss_sales_price=pa.field('ss_sales_price', pa.float64(), nullable=False)
ss_net_profit=pa.field('ss_net_profit', pa.float64(), nullable=False)


#Customer demographics table - RB
cd=[0,2,3]
cd_type = ['int64','string', 'string']
cd_demo_sk=pa.field('cd_demo_sk', pa.int64(), nullable=False)
cd_marital_status=pa.field('cd_marital_status', pa.utf8(),nullable=False)
cd_education_status=pa.field('cd_education_status', pa.utf8(), nullable= False)

#Date dim table - RB
dt=[0,6]
dt_type=['int64','int64']
d_date_sk=pa.field('d_date_sk',pa.int64(), nullable=False)
d_year=pa.field('d_year', pa.int64(),nullable=False)

#Store Fact table - RB
s=[0]
s_type=['int64']
s_store_sk=pa.field('s_store_sk', pa.int64(), nullable=False)

#Customer address table - RB
ca=[0,8,10]
ca_type=['int64','string','string']
ca_address_sk=pa.field('ca_address_sk', pa.int64(), nullable=False)
ca_state=pa.field('ca_state', pa.utf8(), nullable=False)
ca_country=pa.field('ca_country', pa.utf8(), nullable=False)

# Add metadata to only strings.
field_metadata = {b'fletcher_epc' : b'20'}

ca_country=ca_country.add_metadata(field_metadata)
ca_state=ca_state.add_metadata(field_metadata)

cd_marital_status=cd_marital_status.add_metadata(field_metadata)
cd_education_status=cd_education_status.add_metadata(field_metadata)

# Register schema fields
ss_schema_fields = [ss_sold_date_sk, ss_cdemo_sk,ss_addr_sk, ss_store_sk,ss_quantity, ss_sales_price, ss_net_profit]
s_schema_fields = [s_store_sk]
ca_schema_fields = [ca_address_sk,ca_country,ca_state]
cd_schema_fields = [cd_demo_sk, cd_education_status, cd_marital_status]
dt_schema_fields = [d_date_sk, d_year]

# Create a new schema from the fields.
ss_schema = pa.schema(ss_schema_fields)
s_schema = pa.schema(s_schema_fields)
ca_schema = pa.schema(ca_schema_fields)
cd_schema = pa.schema(cd_schema_fields)
dt_schema = pa.schema(dt_schema_fields)

schema_list = [ss_schema, s_schema, ca_schema, cd_schema,dt_schema]

# Construct some metadata to explain Fletchgen that it 
# should allow the FPGA kernel to read from this schema.
metadata = {b'fletcher_mode': b'read',
            b'fletcher_name': b'tpc'}

# Add the metadata to the schema
for s_s in schema_list:
    s_s = s_s.add_metadata(metadata)

# Create a list of PyArrow Arrays. Every Array can be seen 
# as a 'Column' of the RecordBatch we will create.
ss_prefix = data.prefix+ "/dataset/store_sales.dat"
s_prefix =  data.prefix+ "/dataset/store.dat"
cd_prefix = data.prefix+ "/dataset/customer_demographics.dat"
ca_prefix = data.prefix+ "/dataset/customer_address.dat"
dt_prefix = data.prefix+ "/dataset/date_dim.dat"
file_list = [ss_prefix,s_prefix, ca_prefix,cd_prefix,dt_prefix]


#table_column_type_list=[ss_type,cd_type,dt_type,ca_type]

from pyarrow import csv
table_column_index_list=[ss,s,ca,cd,dt]
overall_datas = []
for file_counter,fn in enumerate(file_list):
    data = [] 
    opt = csv.ParseOptions()
    opt.delimiter='|'
    table = csv.read_csv(fn, parse_options=opt)
    #df = table.to_pandas()
    #print(df.head)
    #Iterate over each column
    for i,_ in enumerate(table):
        if i in table_column_index_list[file_counter]: 
            data.append(pa.array(table.column(i).to_pylist()))
            #print(columns.chunk(0))
    overall_datas.append(data)

# Create a RecordBatch from the Arrays.
recordbatch_names = ['ss_recordbatch.rb','s_recordbatch.rb','ca_recordbatch.rb','cd_recordbatch.rb','dt_recordbatch.rb' ] 
if DEBUG:
    print(len(overall_datas))
    print(len(recordbatch_names))
for i in range(len(recordbatch_names)):
    if DEBUG:
        print(f"Processing : {recordbatch_names[i]}")
    recordbatch = pa.RecordBatch.from_arrays(overall_datas[i], schema=schema_list[i])

    # Create an Arrow RecordBatchFileWriter.
    writer = pa.RecordBatchFileWriter(data.prefix + "/" + recordbatch_names[i], schema_list[i])

    # Write the RecordBatch.
    writer.write(recordbatch)

    # Close the writer.
    writer.close()

