from TPCDATASET import *

field_metadata = {b'fletcher_epc' : b'20'}
#
##
#ss_metadata = {b'fletcher_mode': b'read',
#            b'fletcher_name': b'ss'}
#ss = [0,4,6,7,10,13,22]
#ss_obj = store_sales(columns = ss, metadata_information=ss_metadata, field_metadata=field_metadata, metadata_indexes=ss).stream_to_file()
##
#cd_metadata = {b'fletcher_mode': b'read',
#            b'fletcher_name': b'cd'}
#cd=[0,2,3]
#cd_obj = customer_demographics(columns = cd, metadata_information=cd_metadata, field_metadata=field_metadata, metadata_indexes=cd).stream_to_file()
##
#dt_metadata = {b'fletcher_mode': b'read',
#            b'fletcher_name': b'd'}
#dt=[0,6]
#dt_obj = date_dim(columns = dt, metadata_information=dt_metadata, field_metadata=field_metadata, metadata_indexes=dt).stream_to_file()
##
#s_metadata = {b'fletcher_mode': b'read',
#            b'fletcher_name': b's'}
#s=[0]
#s_obj = store(columns = s, metadata_information=s_metadata, field_metadata=field_metadata, metadata_indexes=s).stream_to_file()
##
#ca_metadata = {b'fletcher_mode': b'read',
#            b'fletcher_name': b'ca'}
#ca=[0,8,10]
#ca_obj = customer_address(columns = ca, metadata_information=ca_metadata, field_metadata=field_metadata, metadata_indexes=ca).stream_to_file()

ss_metadata = {b'fletcher_mode': b'read',
            b'fletcher_name': b'ss'}
ss = [7,10]
ss_obj = store_sales(columns = ss, metadata_information=ss_metadata, field_metadata=field_metadata, metadata_indexes=ss).stream_to_file()

s_metadata = {b'fletcher_mode': b'read',
            b'fletcher_name': b's'}
s=[0]
s_obj = store(columns = s, metadata_information=s_metadata, field_metadata=field_metadata, metadata_indexes=s).stream_to_file()
