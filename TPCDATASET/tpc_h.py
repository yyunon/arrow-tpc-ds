import os
import pyarrow as pa
from pyarrow import csv
from TPCDATASET.dataset import Dataset

# TPCH


class lineitem(Dataset):

    def __init__(self, columns=None, row_size=None, c_prefix=None, metadata_information=None, field_metadata=None, metadata_indexes=None, convert_to_fixed=False, convert_date_to_int=False, custom_date_encoding=False):
        if metadata_information == None:
            assert(False)
        super().__init__(c_prefix, metadata_information)
        self.field_metadata = field_metadata
        self.metadata_information = metadata_information
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
            'l_orderkey': pa.field('orderkey', pa.int64(), nullable=False),
            'l_partkey': pa.field('partkey', pa.int64(), nullable=False),
            'l_suppkey': pa.field('suppkey', pa.int64(), nullable=False),
            'l_linenumber': pa.field('linenumber', pa.int64(), nullable=False),
            'l_quantity': pa.field('quantity', pa.float64(), nullable=False),
            'l_extendedprice': pa.field('extendedprice', pa.float64(), nullable=False),
            'l_discount': pa.field('discount', pa.float64(), nullable=False),
            'l_tax': pa.field('tax', pa.float64(), nullable=False),
            'l_returnflag': pa.field('returnflag', pa.utf8(), nullable=False),
            'l_linestatus': pa.field('linestatus', pa.utf8(), nullable=False),
            'l_shipdate': pa.field('shipdate', pa.date32(), nullable=False),
            'l_commitdate': pa.field('commitdate', pa.date32(), nullable=False),
            'l_receiptdate': pa.field('receiptdate', pa.date32(), nullable=False),
            'l_shipinstruct': pa.field('shipinstruct', pa.utf8(), nullable=False),
            'l_shipmode': pa.field('shipmode', pa.utf8(), nullable=False),
            'l_comment': pa.field('comment', pa.utf8(), nullable=False),
        }
        self.pq_table = None
        if metadata_indexes != None:
            print(f"Adding field metadata to indexes: ")
            for i in metadata_indexes:
                print(f"{self.l_columns_index[i]}")
                self.l_columns[self.l_columns_index[i]] = self.l_columns[self.l_columns_index[i]].with_metadata(
                    self.field_metadata)
        self.schema_vars = []
        for i in columns:
            self.schema_vars.append(self.l_columns[self.l_columns_index[i]])
        self.schema = pa.schema(
            self.schema_vars, metadata=self.metadata_information)
        # self.schema.add_metadata(self.metadata_information)
        # Field metadata such as epc, not always applicable
        # output file names
        self.recordbatch_name = "l_recordbatch.rb"
        self.database_name = "dataset/lineitem.dat"
        self.options = "|"
        if columns == None:
            print("Reading all columns...")
            self.table = csv.read_csv(
                self.prefix + self.database_name, parse_options=self.opt)
            for i, _ in enumerate(self.table):
                if i in columns:
                    self.data.append(
                        pa.array(self.table.column(i).to_pylist()))
        else:
            print(f"Reading some columns : {columns}")
            self.table = csv.read_csv(
                self.prefix + self.database_name, parse_options=self.opt)
            print(
                f"Size of read table is: {len(self.table)} rows. You have selected to stream {row_size} number of rows.")
            for i, _ in enumerate(self.table):
                if i in columns:
                    if row_size == None:
                        self.row_size = len(self.table.column(i).to_pylist())
                    temp_read = self.table.column(i).to_pylist()
                    if convert_date_to_int == True and (i == 10 or i == 11 or i == 12):
                        dates = []
                        for i in range(len(temp_read)):
                            if custom_date_encoding == True:
                                def to_integer(dt_time):
                                    return 10000*dt_time.year + 100*dt_time.month + dt_time.day
                                dates.append(to_integer(temp_read[i]))
                            else:
                                import datetime
                                import numpy as np
                                epoch = datetime.datetime(1970, 1, 1)

                                def unix_time_millis(dt):
                                    return np.int32((dt - epoch).days)
                                dates.append(unix_time_millis(temp_read[i]))
                        temp_read = dates
                    if (i == 4 or i == 5 or i == 6 or i == 7):
                        temp_read = [float(i) for i in temp_read]
                    if convert_to_fixed == True and (i == 4 or i == 5 or i == 6 or i == 7):
                        temp_read = [float(i) for i in temp_read]

                        def to_fixed(f, e):
                            a = f * (2**e)
                            b = int(round(a))
                            if a < 0:
                                # next three lines turns b into it's 2's complement.
                                b = abs(b)
                                b = ~b
                                b = b + 1
                            return b
                        fixed_pts = []
                        for i in range(len(temp_read)):
                            y_fxp = to_fixed(temp_read[i], 18)
                            fixed_pts.append(y_fxp)
                        temp_read = fixed_pts
                    self.txt_data.append(temp_read[0:row_size])
                    self.data.append(pa.array(temp_read[0:row_size]))

    def stream_to_parquet(self):
        import pyarrow.parquet as pq
        print(f"Streaming to file parquet.....")
        self.pq_table = pa.Table.from_arrays(self.data, schema=self.schema)
        # Create an Arrow RecordBatchFileWriter.
        self.writer = pq.write_table(
            self.pq_table, self.dir_name + "/lineitem.parquet")

    def stream_to_file(self):
        print(f"Streaming to file {self.recordbatch_name}.....")
        print(self.data)
        self.recordbatch = pa.RecordBatch.from_arrays(
            self.data, schema=self.schema)

        # Create an Arrow RecordBatchFileWriter.
        self.writer = pa.RecordBatchFileWriter(
            self.dir_name + "/" + self.recordbatch_name, self.schema)

        # Write the RecordBatch.
        self.writer.write(self.recordbatch)

        # Close the writer.
        self.writer.close()
        print(
            f"Recordbatch created... {self.dir_name}/{self.recordbatch_name}/{self.recordbatch_name}.....")

    def print_col(self, column):
        assert(self.data != None)
        print(f"Column: {column}, Row:{self.data[column]} ")
