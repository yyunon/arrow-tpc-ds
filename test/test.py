from TPCDATASET.dataset import store_sales,store,customer_demographics,customer_address,date_dim

field_metadata = {b'fletcher_epc' : b'20'}

#
ss_metadata = {b'fletcher_mode': b'read',
               b'fletcher_name': b'ss'}
ss = [0,4,6,7,10,13,22]
ss_obj = store_sales(columns = ss, row_size = 20, metadata_information=ss_metadata, field_metadata=field_metadata)
ss_obj.stream_to_txt_raw(ss_obj.py_list(),"ss.txt")
ss_obj.stream_to_file()

#
#cd_metadata = {b'fletcher_mode': b'read',
#               b'fletcher_name': b'cd'}
#cd=[0,2,3]
#m_cd = [2,3]
#cd_obj = customer_demographics(columns = cd, metadata_information=cd_metadata, field_metadata=field_metadata, metadata_indexes=m_cd).stream_to_file()
##
#dt_metadata = {b'fletcher_mode': b'read',
#               b'fletcher_name': b'd'}
#dt=[0,6]
#dt_obj = date_dim(columns = dt, metadata_information=dt_metadata, field_metadata=field_metadata).stream_to_file()
##
#s_metadata = {b'fletcher_mode': b'read',
#              b'fletcher_name': b's'}
#s=[0]
#s_obj = store(columns = s, metadata_information=s_metadata, field_metadata=field_metadata).stream_to_file()
##
#ca_metadata = {b'fletcher_mode': b'read',
#               b'fletcher_name': b'ca'}
#ca=[0,8,10]
#m_ca=[8,10]
#ca_obj = customer_address(columns = ca, metadata_information=ca_metadata, field_metadata=field_metadata, metadata_indexes=m_ca).stream_to_file()
