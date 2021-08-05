// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

/*
 * Author: Lars van Leeuwen
 * Code for benchmarking the performance of Arrow's Parquet function
 * Lots of the code here is based on example code provided in the Parquet GitHub repo (https://github.com/apache/parquet-cpp/)
 */

#include <string.h>
#include <stdlib.h>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <ctime>
#include <cmath>
#include <random>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/properties.h>
#include <parquet/file_reader.h>
#include <parquet/types.h>
#include <string.h>
#include <limits>

//Struct for timing code
#include "../utils/timer.h"

#define MILLIS_IN_SEC (1000)
#define MILLIS_IN_MIN (60 * MILLIS_IN_SEC)
#define MILLIS_IN_HOUR (60 * MILLIS_IN_MIN)
#define MILLIS_IN_DAY (24 * MILLIS_IN_HOUR)
#define MILLIS_IN_WEEK (7 * MILLIS_IN_DAY)

#define MILLIS_TO_SEC(millis) ((millis) / MILLIS_IN_SEC)
#define MILLIS_TO_MINS(millis) ((millis) / MILLIS_IN_MIN)
#define MILLIS_TO_HOUR(millis) ((millis) / MILLIS_IN_HOUR)
#define MILLIS_TO_DAY(millis) ((millis) / MILLIS_IN_DAY)
#define MILLIS_TO_WEEK(millis) ((millis) / MILLIS_IN_WEEK)

// Write out the data as a Parquet file
void write_parquet_file(const arrow::Table &table, std::string filename, int chunk_size, bool compression, bool dictionary)
{
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(filename));

    auto builder = std::make_shared<parquet::WriterProperties::Builder>();

    builder->encoding(parquet::Encoding::PLAIN);

    builder->max_row_group_length(std::numeric_limits<int>::max());

    builder->data_pagesize(1000);

    builder->disable_statistics();

    //Parquet options
    if (compression)
    {
        builder->compression(parquet::Compression::SNAPPY);
    }
    else
    {
        builder->compression(parquet::Compression::UNCOMPRESSED);
    }

    if (dictionary)
    {
        builder->enable_dictionary();
    }
    else
    {
        builder->disable_dictionary();
    }

    std::shared_ptr<parquet::WriterProperties> props = builder->build();

    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, chunk_size, props));
    std::cout << "Finished...\n";
}
time_t Epoch()
{
    // HACK: MSVC mktime() fails on UTC times before 1970-01-01 00:00:00.
    // But it first converts its argument from local time to UTC time,
    // so we ask for 1970-01-02 to avoid failing in timezones ahead of UTC.
    struct tm y1970;
    memset(&y1970, 0, sizeof(struct tm));
    y1970.tm_year = 70;
    y1970.tm_mon = 0;
    y1970.tm_mday = 2;
    y1970.tm_hour = 0;
    y1970.tm_min = 0;
    y1970.tm_sec = 0;
    time_t epoch = mktime(&y1970);
    if (epoch == static_cast<time_t>(-1))
    {
        std::cout << "mktime() failed";
    }
    // Adjust for the 24h offset above.
    return epoch - 24 * 3600;
}
int32_t DaysSince(time_t base_line, int32_t yy, int32_t mm, int32_t dd, int32_t hr,
                  int32_t min, int32_t sec, int32_t millis)
{
    struct tm given_ts;
    memset(&given_ts, 0, sizeof(struct tm));
    given_ts.tm_year = (yy - 1900);
    given_ts.tm_mon = (mm - 1);
    given_ts.tm_mday = dd;
    given_ts.tm_hour = hr;
    given_ts.tm_min = min;
    given_ts.tm_sec = sec;

    time_t ts = mktime(&given_ts);
    if (ts == static_cast<time_t>(-1))
    {
        std::cout << "mktime() failed";
    }
    // time_t is an arithmetic type on both POSIX and Windows, we can simply
    // subtract to get a duration in seconds.
    return static_cast<int32_t>(((ts - base_line) * 1000 + millis) / MILLIS_IN_DAY);
}
int32_t DayConverter(const char *inp)
{
    time_t epoch = Epoch();
    // parse date
    std::vector<int32_t> tok;
    //char *pch = const_cast<char *>(splittedString[10].c_str());
    char *pch = const_cast<char *>(inp);
    char *nextToken = nullptr;
    nextToken = strtok(pch, "-");
    while (nextToken)
    {
        tok.push_back(std::stoi(nextToken));
        nextToken = strtok(NULL, "-");
    }

    return DaysSince(epoch, tok[0], tok[1], tok[2], 0, 0, 0, 0);
}
int main(int argc, char **argv)
{
    //if (argc < 0)
    //{
    //    std::cout << "Usage: prelim num_values" << std::endl;
    //    std::cout << "Usage: Provide a $DATASET_DIR environment variable which holds the csv file of lineitem" << std::endl;
    //    return 1;
    //}

    arrow::Int64Builder orderkey_builder, partkey_builder, suppkey_builder, linenumber_builder;
    arrow::DoubleBuilder ext_builder, disc_builder, quant_builder, tax_builder;
    arrow::StringBuilder returnflag_builder, linestatus_builder, shipinstruct_builder, shipmode_builder, comment_builder;
    arrow::Date32Builder ship_builder, commit_builder, receipt_builder;

    std::shared_ptr<arrow::Int64Array> orderkey_array, partkey_array, suppkey_array, linenumber_array;
    std::shared_ptr<arrow::DoubleArray> ext_array, disc_array, quant_array, tax_array;
    std::shared_ptr<arrow::Array> returnflag_array, linestatus_array, shipinstruct_array, shipmode_array, comment_array;
    std::shared_ptr<arrow::Date32Array> ship_array, commit_array, receipt_array;

    int num_values = atoi(argv[1]);
    //int num_values = 10000;
    // Read csv. file
    const char *tmp = std::getenv("DATASET_DIR");
    if (tmp == nullptr)
    {
        std::cerr << "DATASET_DIR env var does not exits\n";
        return -1;
    }
    std::string name_dat = "/lineitem.tbl";
    std::string dataset_dir(tmp ? tmp : "");
    std::string path_str = dataset_dir + name_dat;

    //std::ofstream dec_check_file;
    //dec_check_file.open("decoded_lineitem.dec");

    std::fstream infile(path_str);
    //char buffer[65536];
    char buffer[4096];

    infile.rdbuf()->pubsetbuf(buffer, sizeof(buffer));
    std::string line;
    std::vector<std::string> splittedString;

    uint64_t row_count = 0;
    bool selected_rows = (num_values < 0) ? false : true;
    while (getline(infile, line))
    {
        if (selected_rows & (row_count == num_values))
            break;
        splittedString.clear();
        size_t last = 0, pos = 0;
        while ((pos = line.find('|', last)) != std::string::npos)
        {
            splittedString.emplace_back(line, last, pos - last);
            last = pos + 1;
        }
        if (last)
            splittedString.emplace_back(line, last);
        PARQUET_THROW_NOT_OK(orderkey_builder.Append(stoi(splittedString[0])));
        PARQUET_THROW_NOT_OK(partkey_builder.Append(stoi(splittedString[1])));
        PARQUET_THROW_NOT_OK(suppkey_builder.Append(stoi(splittedString[2])));
        PARQUET_THROW_NOT_OK(linenumber_builder.Append(stoi(splittedString[3])));
        PARQUET_THROW_NOT_OK(quant_builder.Append(stod(splittedString[4])));
        PARQUET_THROW_NOT_OK(ext_builder.Append(stod(splittedString[5])));
        PARQUET_THROW_NOT_OK(disc_builder.Append(stod(splittedString[6])));
        PARQUET_THROW_NOT_OK(tax_builder.Append(stod(splittedString[7])));
        PARQUET_THROW_NOT_OK(returnflag_builder.Append(splittedString[8]));
        PARQUET_THROW_NOT_OK(linestatus_builder.Append(splittedString[9]));
        // Date Types
        PARQUET_THROW_NOT_OK(ship_builder.Append(DayConverter(splittedString[10].c_str())));
        PARQUET_THROW_NOT_OK(commit_builder.Append(DayConverter(splittedString[11].c_str())));
        PARQUET_THROW_NOT_OK(receipt_builder.Append(DayConverter(splittedString[12].c_str())));

        PARQUET_THROW_NOT_OK(shipinstruct_builder.Append(splittedString[13]));
        PARQUET_THROW_NOT_OK(shipmode_builder.Append(splittedString[14]));
        PARQUET_THROW_NOT_OK(comment_builder.Append(splittedString[15]));

        // Append to debug file too
        //dec_check_file << stod(splittedString[4]) << "," << stod(splittedString[5]) << "," << stod(splittedString[6]) << "," << s_d << "\n";

        ++row_count;
    }
    std::cout << row_count << " rows read...\n";

    PARQUET_THROW_NOT_OK(orderkey_builder.Finish(&orderkey_array));
    PARQUET_THROW_NOT_OK(partkey_builder.Finish(&partkey_array));
    PARQUET_THROW_NOT_OK(suppkey_builder.Finish(&suppkey_array));
    PARQUET_THROW_NOT_OK(linenumber_builder.Finish(&linenumber_array));
    PARQUET_THROW_NOT_OK(quant_builder.Finish(&quant_array));
    PARQUET_THROW_NOT_OK(ext_builder.Finish(&ext_array));
    PARQUET_THROW_NOT_OK(disc_builder.Finish(&disc_array));
    PARQUET_THROW_NOT_OK(tax_builder.Finish(&tax_array));
    PARQUET_THROW_NOT_OK(returnflag_builder.Finish(&returnflag_array));
    PARQUET_THROW_NOT_OK(linestatus_builder.Finish(&linestatus_array));
    PARQUET_THROW_NOT_OK(ship_builder.Finish(&ship_array));
    PARQUET_THROW_NOT_OK(commit_builder.Finish(&commit_array));
    PARQUET_THROW_NOT_OK(receipt_builder.Finish(&receipt_array));
    PARQUET_THROW_NOT_OK(shipinstruct_builder.Finish(&shipinstruct_array));
    PARQUET_THROW_NOT_OK(shipmode_builder.Finish(&shipmode_array));
    PARQUET_THROW_NOT_OK(comment_builder.Finish(&comment_array));

    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("l_orderkey", arrow::int64(), false),
                                                           arrow::field("l_partkey", arrow::int64(), false),
                                                           arrow::field("l_suppkey", arrow::int64(), false),
                                                           arrow::field("l_linenumber", arrow::int64(), false),
                                                           arrow::field("l_quantity", arrow::float64(), false),
                                                           arrow::field("l_extendedprice", arrow::float64(), false),
                                                           arrow::field("l_discount", arrow::float64(), false),
                                                           arrow::field("l_tax", arrow::float64(), false),
                                                           arrow::field("l_returnflag", arrow::utf8(), false),
                                                           arrow::field("l_linestatus", arrow::utf8(), false),
                                                           arrow::field("l_shipdate", arrow::date32(), false),
                                                           arrow::field("l_commitdate", arrow::date32(), false),
                                                           arrow::field("l_receiptdate", arrow::date32(), false),
                                                           arrow::field("l_shipinstruct", arrow::utf8(), false),
                                                           arrow::field("l_shipmode", arrow::utf8(), false),
                                                           arrow::field("l_comment", arrow::utf8(), false)});

    std::vector<std::shared_ptr<arrow::Array>> output_arrs = {orderkey_array, partkey_array, suppkey_array, linenumber_array, quant_array, ext_array, disc_array, tax_array, returnflag_array, linestatus_array, ship_array, commit_array, receipt_array, shipinstruct_array, shipmode_array, comment_array};

    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, output_arrs);

    write_parquet_file(*table, "lineitem.parquet", row_count, false, false);

    //dec_check_file.close();
    return 0;
}
