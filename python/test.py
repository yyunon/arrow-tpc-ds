from TPCDATASET import *

field_metadata = {b'fletcher_epc' : b'20'}
metadata = {b'fletcher_mode': b'read',
            b'fletcher_name': b'store_sales'}

ss = [0,4,6,7,10,13,22]
ss = store_sales(columns = ss, metadata_information=metadata, field_metadata=field_metadata, metadata_indexes=ss)

