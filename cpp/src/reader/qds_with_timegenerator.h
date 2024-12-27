/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef READER_QDS_WITH_TIMEGENERATOR_H
#define READER_QDS_WITH_TIMEGENERATOR_H

#include "common/db_common.h"
#include "expression.h"
#include "reader/tsfile_series_scan_iterator.h"
#include "result_set.h"

namespace storage {

class TimeGtEq;

enum NodeType { LEAF_NODE = 0, AND_NODE, OR_NODE };

static const int64_t INVALID_NEXT_TIMESTAMP = -1;

struct SeriesScanStream {
    TsFileIOReader *io_reader_;
    TsFileSeriesScanIterator *ssi_;
    common::TsBlock *tsblock_;
    common::ColIterator *col_iter_;

    SeriesScanStream()
        : io_reader_(nullptr),
          ssi_(nullptr),
          tsblock_(nullptr),
          col_iter_(nullptr) {}
    int init();
    void destroy();
    int64_t front();
    void pop_front(int64_t beyond_this_time);

   private:
    int64_t read_timestamp();
};

struct ValueAt {
    TsFileSeriesScanIterator *ssi_;
    common::TsBlock *tsblock_;
    common::ColIterator *time_col_iter_;
    common::ColIterator *value_col_iter_;
    int64_t cur_time_;
    TimeGtEq *tf_;
    common::TSDataType data_type_;
    TsFileIOReader *io_reader_;

    ValueAt()
        : ssi_(nullptr),
          tsblock_(nullptr),
          time_col_iter_(nullptr),
          value_col_iter_(nullptr),
          cur_time_(-1),
          tf_(nullptr),
          data_type_(common::INVALID_DATATYPE),
          io_reader_(nullptr) {}
    // get value object pointer at time @target_timestamp
    // if no such TV exists, return nullptr
    void *at(int64_t target_timestamp);
    void destroy();
};

enum NextDirection {
    __INVALID_NEXT,
    LEFT_NEXT,
    RIGHT_NEXT,
    BOTH_NEXT,
    STOP_NEXT
};

struct Node {
    Node *left_;
    Node *right_;
    SeriesScanStream sss_;
    NodeType type_;
    NextDirection next_direction_;

    explicit Node(NodeType type)
        : left_(nullptr),
          right_(nullptr),
          sss_(),
          type_(type),
          next_direction_(__INVALID_NEXT) {}
    ~Node() { destroy(); }
    void destroy() { sss_.destroy(); }

    // if not exist, return INVALID_NEXT_TIMESTAMP
    int64_t get_cur_timestamp();
    void next_timestamp(int64_t beyond_this_time);
};

class QDSWithTimeGenerator : public ResultSet {
   public:
    QDSWithTimeGenerator()
        : row_record_(nullptr),
          result_set_metadata_(nullptr),
          io_reader_(nullptr),
          qe_(nullptr),
          tree_(nullptr),
          value_at_vec_() {}
    ~QDSWithTimeGenerator() { close(); }

    int init(TsFileIOReader *io_reader, QueryExpression *qe);
    void close();
    bool next();
    bool is_null(const std::string &column_name);
    bool is_null(uint32_t column_index);
    RowRecord *get_row_record();
    ResultSetMetadata *get_metadata();

   private:
    Node *construct_node_tree(Expression *expr);

   private:
    RowRecord *row_record_;
    ResultSetMetadata *result_set_metadata_;
    TsFileIOReader *io_reader_;
    QueryExpression *qe_;
    Node *tree_;
    std::vector<ValueAt> value_at_vec_;
};

}  // namespace storage

#endif  // READER_QDS_WITH_TIMEGENERATOR_H