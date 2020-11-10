import pyarrow as pa

#Store Sales table
ss = [0,4,6,7,10,13,22]
ss_type = ['int32','int32','int32','int32','int32','float64','float64']
ss_sold_date_sk = pa.field('ss_sold_date_sk', pa.int32(), nullable=False)
ss_cdemo_sk= pa.field('ss_cdemo_sk', pa.int32(), nullable=False)
ss_addr_sk= pa.field('ss_addr_sk', pa.int32(), nullable=False)
ss_store_sk= pa.field('ss_store_sk', pa.int32(), nullable=False)
ss_quantity= pa.field('ss_quantity', pa.int32(), nullable=False)
ss_sales_price=pa.field('ss_sales_price', pa.float64(), nullable=False)
ss_net_profit=pa.field('ss_net_profit', pa.float64(), nullable=False)

#Customer demographics table
cd=[0,2,3]
cd_type = ['int32','string', 'string']
cd_demo_sk=pa.field('cd_demo_sk', pa.int32(), nullable=False)
cd_marital_status=pa.field('cd_marital_status', pa.utf8(),nullable=False)
cd_education_status=pa.field('cd_education_status', pa.utf8(), nullable= False)

#Date dim table
dt=[0,6]
dt_type=['int32','int32']
d_date_sk=pa.field('d_date_sk',pa.int32(), nullable=False)
d_year=pa.field('d_year', pa.int32(),nullable=False)

#Store Fact table
#s=[0]
#s_type=['int32']
#s_store_sk=pa.field('s_store_sk', pa.int32(), nullable=False)

#Customer address table
ca=[0,8,10]
ca_type=['int32','string','string']
ca_address_sk=pa.field('ca_address_sk', pa.int32(), nullable=False)
ca_state=pa.field('ca_state', pa.utf8(), nullable=False)
ca_country=pa.field('ca_country', pa.utf8(), nullable=False)

field_metadata = {b'fletcher_epc' : b'20'}

#ss_sold_date_sk=ss_sold_date_sk.add_metadata(field_metadata)
#ss_cdemo_sk=ss_cdemo_sk.add_metadata(field_metadata)
#ss_addr_sk=ss_addr_sk.add_metadata(field_metadata)
#ss_store_sk=ss_store_sk.add_metadata(field_metadata)
#ss_quantity=ss_quantity.add_metadata(field_metadata)
#ss_sales_price=ss_sales_price.add_metadata(field_metadata)
#ss_net_profit=ss_net_profit.add_metadata(field_metadata)

#cd_demo_sk=cd_demo_sk.add_metadata(field_metadata)
cd_marital_status=cd_marital_status.add_metadata(field_metadata)
cd_education_status=cd_education_status.add_metadata(field_metadata)

#d_year=d_year.add_metadata(field_metadata)
#d_date_sk=d_date_sk.add_metadata(field_metadata)

#s_store_sk=s_store_sk.add_metadata(field_metadata)

#ca_address_sk=ca_address_sk.add_metadata(field_metadata)
ca_country=ca_country.add_metadata(field_metadata)
ca_state=ca_state.add_metadata(field_metadata)

# Create a list of fields for pa.schema()
schema_fields = [ss_sold_date_sk, ss_cdemo_sk,ss_addr_sk, ss_store_sk,ss_quantity, ss_sales_price, ss_net_profit,cd_demo_sk,cd_marital_status,cd_education_status,d_year, d_date_sk, ca_address_sk, ca_country, ca_state]

# Create a new schema from the fields.
schema = pa.schema(schema_fields)

# Construct some metadata to explain Fletchgen that it 
# should allow the FPGA kernel to read from this schema.
metadata = {b'fletcher_mode': b'read',
            b'fletcher_name': b'tpc'}

# Add the metadata to the schema
schema = schema.add_metadata(metadata)

# Create a list of PyArrow Arrays. Every Array can be seen 
# as a 'Column' of the RecordBatch we will create.
ss_prefix = "/home/yyunon/thesis_journals/resources/tpc-ds-sql/v2.13.0rc1/dataset/store_sales.dat"
#s_prefix = "/home/yyunon/thesis_journals/resources/tpc-ds-sql/v2.13.0rc1/dataset/store.dat"
cd_prefix = "/home/yyunon/thesis_journals/resources/tpc-ds-sql/v2.13.0rc1/dataset/customer_demographics.dat"
ca_prefix = "/home/yyunon/thesis_journals/resources/tpc-ds-sql/v2.13.0rc1/dataset/customer_address.dat"
dt_prefix = "/home/yyunon/thesis_journals/resources/tpc-ds-sql/v2.13.0rc1/dataset/date_dim.dat"

file_list = [ss_prefix,cd_prefix, dt_prefix, ca_prefix]
table_column_index_list=[ss,cd,dt,ca]
table_column_type_list=[ss_type,cd_type,dt_type,ca_type]
"""
int32,
float64,
string
"""
#table_lines_to_read=[1000,1000.1000,1000]
file_counter=0
data = [] 
for i in file_list:
    with open(i, 'r') as filess:
        content = filess.readlines()
    content = [x.strip() for x in content]
    column_counter = 0
    for column in table_column_index_list[file_counter]:
        column_fields = []
        column_type = table_column_type_list[file_counter][column_counter]
        for row in range(int(len(content))):
        #for row in range(10000):
            val = content[row].split('|')[column]
            if column_type == 'int32':
                if val =='':
                    val = 0
                val = int(val)
            elif column_type == 'float64':
                if val =='':
                    val = 0
                val = float(val)
            elif column_type == 'string':
                if val =='':
                    val = ' ' 
            column_fields.append(val)
        column_counter+=1
        data.append(pa.array(column_fields))
    file_counter+=1
print(data)
print(len(data))
# Create a RecordBatch from the Arrays.
recordbatch = pa.RecordBatch.from_arrays(data, schema)

# Create an Arrow RecordBatchFileWriter.
writer = pa.RecordBatchFileWriter('recordbatch.rb', schema)

# Write the RecordBatch.
writer.write(recordbatch)

# Close the writer.
writer.close()

