/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */
#include "Common/Compat.h"
#include "Common/Init.h"
#include "Common/Logger.h"
#include "Common/Mutex.h"
#include "Common/Random.h"
#include "HyperAppHelper/Unique.h"

#include <iostream>
#include <iomanip>
#include <sstream>

#include <boost/shared_ptr.hpp>

#include <concurrency/ThreadManager.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TBufferTransports.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>

#include "Common/Time.h"
#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/HqlInterpreter.h"
#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/NamespaceListing.h"
#include "Hypertable/Lib/Future.h"

#include "Config.h"
#include "SerializedCellsReader.h"
#include "SerializedCellsWriter.h"
#include "ThriftHelper.h"

#define THROW_TE(_code_, _str_) do { ThriftGen::ClientException te; \
  te.code = _code_; te.message = _str_; \
  te.__isset.code = te.__isset.message = true; \
  throw te; \
} while (0)

#define THROW_(_code_) do { \
  Hypertable::Exception e(_code_, __LINE__, HT_FUNC, __FILE__); \
  std::ostringstream oss; oss << e; \
  HT_ERROR_OUT << oss.str() << HT_END; \
  THROW_TE(_code_, oss.str()); \
} while (0)

#define RETHROW(_expr_) catch (Hypertable::Exception &e) { \
  std::ostringstream oss;  oss << HT_FUNC << _expr_ << ": "<< e; \
  HT_ERROR_OUT << oss.str() << HT_END; \
  THROW_TE(e.code(), oss.str()); \
}

#define LOG_API_START(_expr_) \
  boost::xtime start_time, end_time; \
  std::ostringstream logging_stream;\
  if (m_log_api) {\
    boost::xtime_get(&start_time, TIME_UTC);\
    logging_stream << "API " << __func__ << ": " << _expr_;\
  }

#define LOG_API_FINISH \
  if (m_log_api) { \
    boost::xtime_get(&end_time, TIME_UTC); \
    std::cout << start_time.sec <<'.'<< std::setw(9) << std::setfill('0') << start_time.nsec <<" API "<< __func__ <<": "<< logging_stream.str() << " latency=" << xtime_diff_millis(start_time, end_time) << std::endl; \
  }

#define LOG_API_FINISH_E(_expr_) \
  if (m_log_api) { \
    boost::xtime_get(&end_time, TIME_UTC); \
    std::cout << start_time.sec <<'.'<< std::setw(9) << std::setfill('0') << start_time.nsec <<" API "<< __func__ <<": "<< logging_stream.str() << _expr_ << " latency=" << xtime_diff_millis(start_time, end_time) << std::endl; \
  }


#define LOG_API(_expr_) do { \
  if (m_log_api) \
    std::cout << hires_ts <<" API "<< __func__ <<": "<< _expr_ << std::endl; \
} while (0)

#define LOG_HQL_RESULT(_res_) do { \
  if (m_log_api) \
    cout << hires_ts <<" API "<< __func__ <<": result: "; \
  if (Logger::logger->isDebugEnabled()) \
    cout << _res_; \
  else if (m_log_api) { \
    if (_res_.__isset.results) \
      cout <<"results.size=" << _res_.results.size(); \
    if (_res_.__isset.cells) \
      cout <<"cells.size=" << _res_.cells.size(); \
    if (_res_.__isset.scanner) \
      cout <<"scanner="<< _res_.scanner; \
    if (_res_.__isset.mutator) \
      cout <<"mutator="<< _res_.mutator; \
  } \
  cout << std::endl; \
} while(0)

namespace Hypertable { namespace ThriftBroker {

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;

using namespace Config;
using namespace ThriftGen;
using namespace boost;
using namespace std;

class SharedMutatorMapKey {
public:
  SharedMutatorMapKey(int64_t ns, const String &tablename,
      const ThriftGen::MutateSpec &mutate_spec)
    : m_namespace(ns), m_tablename(tablename), m_mutate_spec(mutate_spec) {}

  int compare(const SharedMutatorMapKey &skey) const {
    int64_t  cmp;

    cmp = m_namespace - skey.m_namespace;
    if (cmp != 0)
      return cmp;
    cmp = m_tablename.compare(skey.m_tablename);
    if (cmp != 0)
      return cmp;
    cmp = m_mutate_spec.appname.compare(skey.m_mutate_spec.appname);
    if (cmp != 0)
      return cmp;
    cmp = m_mutate_spec.flush_interval - skey.m_mutate_spec.flush_interval;
    if (cmp != 0)
      return cmp;
    cmp = m_mutate_spec.flags - skey.m_mutate_spec.flags;
    return cmp;
  }

  int64_t m_namespace;
  String m_tablename;
  ThriftGen::MutateSpec m_mutate_spec;
};

inline bool operator < (const SharedMutatorMapKey &skey1, const SharedMutatorMapKey &skey2) {
  return skey1.compare(skey2) < 0;
}


typedef Meta::list<ThriftBrokerPolicy, DefaultCommPolicy> Policies;

typedef std::map<SharedMutatorMapKey, TableMutatorPtr> SharedMutatorMap;
typedef hash_map< ::int64_t, TableScannerPtr> ScannerMap;
typedef hash_map< ::int64_t, TableScannerAsyncPtr> ScannerAsyncMap;
typedef hash_map< ::int64_t, ::int64_t> ReverseScannerAsyncMap;
typedef hash_map< ::int64_t, TableMutatorPtr> MutatorMap;
typedef hash_map< ::int64_t, TableMutatorAsyncPtr> MutatorAsyncMap;
typedef hash_map< ::int64_t, NamespacePtr> NamespaceMap;
typedef hash_map< ::int64_t, FuturePtr> FutureMap;
typedef hash_map< ::int64_t, HqlInterpreterPtr> HqlInterpreterMap;
typedef std::vector<ThriftGen::Cell> ThriftCells;
typedef std::vector<CellAsArray> ThriftCellsAsArrays;

::int64_t
cell_str_to_num(const std::string &from, const char *label,
                ::int64_t min_num = INT64_MIN, ::int64_t max_num = INT64_MAX) {
  char *endp;

  ::int64_t value = strtoll(from.data(), &endp, 0);

  if (endp - from.data() != (int)from.size()
      || value < min_num || value > max_num)
    HT_THROWF(Error::BAD_KEY, "Error converting %s to %s", from.c_str(), label);

  return value;
}

void
convert_scan_spec(const ThriftGen::ScanSpec &tss, Hypertable::ScanSpec &hss) {
  if (tss.__isset.row_limit)
    hss.row_limit = tss.row_limit;

  if (tss.__isset.cell_limit)
    hss.cell_limit = tss.cell_limit;

  if (tss.__isset.cell_limit_per_family)
    hss.cell_limit_per_family = tss.cell_limit_per_family;

  if (tss.__isset.revs)
    hss.max_versions = tss.revs;

  if (tss.__isset.start_time)
    hss.time_interval.first = tss.start_time;

  if (tss.__isset.end_time)
    hss.time_interval.second = tss.end_time;

  if (tss.__isset.return_deletes)
    hss.return_deletes = tss.return_deletes;

  if (tss.__isset.keys_only)
    hss.keys_only = tss.keys_only;

  if (tss.__isset.row_regexp)
    hss.row_regexp = tss.row_regexp.c_str();

  if (tss.__isset.value_regexp)
    hss.value_regexp = tss.value_regexp.c_str();

  if (tss.__isset.scan_and_filter_rows)
    hss.scan_and_filter_rows = tss.scan_and_filter_rows;

  if (tss.__isset.row_offset)
    hss.row_offset = tss.row_offset;

  if (tss.__isset.cell_offset)
    hss.cell_offset = tss.cell_offset;

  // shallow copy
  foreach(const ThriftGen::RowInterval &ri, tss.row_intervals)
    hss.row_intervals.push_back(Hypertable::RowInterval(ri.start_row.c_str(),
        ri.start_inclusive, ri.end_row.c_str(), ri.end_inclusive));

  foreach(const ThriftGen::CellInterval &ci, tss.cell_intervals)
    hss.cell_intervals.push_back(Hypertable::CellInterval(
        ci.start_row.c_str(), ci.start_column.c_str(), ci.start_inclusive,
        ci.end_row.c_str(), ci.end_column.c_str(), ci.end_inclusive));

  foreach(const std::string &col, tss.columns)
    hss.columns.push_back(col.c_str());
}

void convert_cell(const ThriftGen::Cell &tcell, Hypertable::Cell &hcell) {
  // shallow copy
  if (tcell.key.__isset.row)
    hcell.row_key = tcell.key.row.c_str();

  if (tcell.key.__isset.column_family)
    hcell.column_family = tcell.key.column_family.c_str();

  if (tcell.key.__isset.column_qualifier)
    hcell.column_qualifier = tcell.key.column_qualifier.c_str();

  if (tcell.key.__isset.timestamp)
    hcell.timestamp = tcell.key.timestamp;

  if (tcell.key.__isset.revision)
    hcell.revision = tcell.key.revision;

  if (tcell.__isset.value) {
    hcell.value = (::uint8_t *)tcell.value.c_str();
    hcell.value_len = tcell.value.length();
  }
  if (tcell.key.__isset.flag)
    hcell.flag = tcell.key.flag;
}

void convert_key(const ThriftGen::Key &tkey, Hypertable::KeySpec &hkey) {
  // shallow copy
  if (tkey.__isset.row) {
    hkey.row = tkey.row.c_str();
    hkey.row_len = tkey.row.size();
  }

  if (tkey.__isset.column_family)
    hkey.column_family = tkey.column_family.c_str();

  if (tkey.__isset.column_qualifier)
    hkey.column_qualifier = tkey.column_qualifier.c_str();

  if (tkey.__isset.timestamp)
    hkey.timestamp = tkey.timestamp;

  if (tkey.__isset.revision)
    hkey.revision = tkey.revision;
}

int32_t convert_cell(const Hypertable::Cell &hcell, ThriftGen::Cell &tcell) {
  int32_t amount = sizeof(ThriftGen::Cell);

  tcell.key.row = hcell.row_key;
  amount += tcell.key.row.length();
  tcell.key.column_family = hcell.column_family;
  amount += tcell.key.column_family.length();

  if (hcell.column_qualifier && *hcell.column_qualifier) {
    tcell.key.column_qualifier = hcell.column_qualifier;
    tcell.key.__isset.column_qualifier = true;
    amount += tcell.key.column_qualifier.length();
  }
  else {
    tcell.key.column_qualifier = "";
    tcell.key.__isset.column_qualifier = false;
  }

  tcell.key.timestamp = hcell.timestamp;
  tcell.key.revision = hcell.revision;

  if (hcell.value && hcell.value_len) {
    tcell.value = std::string((char *)hcell.value, hcell.value_len);
    tcell.__isset.value = true;
    amount += hcell.value_len;
  }
  else {
    tcell.value = "";
    tcell.__isset.value = false;
  }

  tcell.key.flag = (KeyFlag::type)hcell.flag;
  tcell.key.__isset.row = tcell.key.__isset.column_family = tcell.key.__isset.timestamp
      = tcell.key.__isset.revision = tcell.key.__isset.flag = true;
  return amount;
}

void convert_cell(const CellAsArray &tcell, Hypertable::Cell &hcell) {
  int len = tcell.size();

  switch (len) {
  case 7: hcell.flag = cell_str_to_num(tcell[6], "cell flag", 0, 255);
  case 6: hcell.revision = cell_str_to_num(tcell[5], "revision");
  case 5: hcell.timestamp = cell_str_to_num(tcell[4], "timestamp");
  case 4: hcell.value = (::uint8_t *)tcell[3].c_str();
          hcell.value_len = tcell[3].length();
  case 3: hcell.column_qualifier = tcell[2].c_str();
  case 2: hcell.column_family = tcell[1].c_str();
  case 1: hcell.row_key = tcell[0].c_str();
    break;
  default:
    HT_THROWF(Error::BAD_KEY, "CellAsArray: bad size: %d", len);
  }
}

int32_t convert_cell(const Hypertable::Cell &hcell, CellAsArray &tcell) {
  int32_t amount = 5*sizeof(std::string);
  tcell.resize(5);
  tcell[0] = hcell.row_key;
  amount += tcell[0].length();
  tcell[1] = hcell.column_family;
  amount += tcell[1].length();
  tcell[2] = hcell.column_qualifier ? hcell.column_qualifier : "";
  amount += tcell[2].length();
  tcell[3] = std::string((char *)hcell.value, hcell.value_len);
  amount += tcell[3].length();
  tcell[4] = format("%llu", (Llu)hcell.timestamp);
  amount += tcell[4].length();
  return amount;
}

int32_t convert_cells(const Hypertable::Cells &hcells, ThriftCells &tcells) {
  // deep copy
  int32_t amount = sizeof(ThriftCells);
  tcells.resize(hcells.size());
  for(size_t ii=0; ii<hcells.size(); ++ii)
    amount += convert_cell(hcells[ii], tcells[ii]);

  return amount;
}

void convert_cells(const ThriftCells &tcells, Hypertable::Cells &hcells) {
  // shallow copy
  foreach(const ThriftGen::Cell &tcell, tcells) {
    Hypertable::Cell hcell;
    convert_cell(tcell, hcell);
    hcells.push_back(hcell);
  }
}

int32_t convert_cells(const Hypertable::Cells &hcells, ThriftCellsAsArrays &tcells) {
  // deep copy
  int32_t amount = sizeof(CellAsArray);
  tcells.resize(hcells.size());
  for(size_t ii=0; ii<hcells.size(); ++ii) {
    amount += convert_cell(hcells[ii], tcells[ii]);
  }
 return amount;
}

int32_t convert_cells(Hypertable::Cells &hcells, CellsSerialized &tcells) {
  // deep copy
  int32_t amount = 0 ;
  for(size_t ii=0; ii<hcells.size(); ++ii) {
    amount += strlen(hcells[ii].row_key) + strlen(hcells[ii].column_family) +
      strlen(hcells[ii].column_qualifier) + 8 + 8 + 4 + 1+ hcells[ii].value_len + 4;
  }
  SerializedCellsWriter writer(amount, true);
  for(size_t ii=0; ii<hcells.size(); ++ii) {
    writer.add(hcells[ii]);
  }
  writer.finalize(SerializedCellsFlag::EOS);
  tcells = String((char *)writer.get_buffer(), writer.get_buffer_length());
  amount = tcells.size();
  return amount;
}

void
convert_cells(const ThriftCellsAsArrays &tcells, Hypertable::Cells &hcells) {
  // shallow copy
  foreach(const CellAsArray &tcell, tcells) {
    Hypertable::Cell hcell;
    convert_cell(tcell, hcell);
    hcells.push_back(hcell);
  }
}

void convert_table_split(const Hypertable::TableSplit &hsplit, ThriftGen::TableSplit &tsplit) {

  /** start_row **/
  if (hsplit.start_row) {
    tsplit.start_row = hsplit.start_row;
    tsplit.__isset.start_row = true;
  }
  else {
    tsplit.start_row = "";
    tsplit.__isset.start_row = false;
  }

  /** end_row **/
  if (hsplit.end_row &&
      !(hsplit.end_row[0] == (char)0xff && hsplit.end_row[1] == (char)0xff)) {
    tsplit.end_row = hsplit.end_row;
    tsplit.__isset.end_row = true;
  }
  else {
    tsplit.end_row = Hypertable::Key::END_ROW_MARKER;
    tsplit.__isset.end_row = false;
  }

  /** location **/
  if (hsplit.location) {
    tsplit.location = hsplit.location;
    tsplit.__isset.location = true;
  }
  else {
    tsplit.location = "";
    tsplit.__isset.location = false;
  }

  /** ip_address **/
  if (hsplit.ip_address) {
    tsplit.ip_address = hsplit.ip_address;
    tsplit.__isset.ip_address = true;
  }
  else {
    tsplit.ip_address = "";
    tsplit.__isset.ip_address = false;
  }

  /** hostname **/
  if (hsplit.hostname) {
    tsplit.hostname = hsplit.hostname;
    tsplit.__isset.hostname = true;
  }
  else {
    tsplit.hostname = "";
    tsplit.__isset.hostname = false;
  }

}


class ServerHandler;

template <class ResultT, class CellT>
struct HqlCallback : HqlInterpreter::Callback {
  typedef HqlInterpreter::Callback Parent;

  ResultT &result;
  ServerHandler &handler;
  bool flush, buffered;

  HqlCallback(ResultT &r, ServerHandler *handler, bool flush, bool buffered)
    : result(r), handler(*handler), flush(flush), buffered(buffered) { }

  virtual void on_return(const String &);
  virtual void on_scan(TableScanner &);
  virtual void on_finish(TableMutator *);
};

class ServerHandler : public HqlServiceIf {
public:
  ServerHandler() {
    m_log_api = Config::get_bool("ThriftBroker.API.Logging");
    m_next_threshold = Config::get_i32("ThriftBroker.NextThreshold");
    m_client = new Hypertable::Client();
    m_next_namespace_id = 1;
    m_next_future_id = 1;
    m_future_queue_size = Config::get_i32("ThriftBroker.Future.QueueSize");
  }

  virtual void
  hql_exec(HqlResult& result, const ThriftGen::Namespace ns, const String &hql, bool noflush,
           bool unbuffered) {
    LOG_API_START("namespace=" << ns << " hql="<< hql <<" noflush="<< noflush
                  << " unbuffered="<< unbuffered);

    try {
      HqlCallback<HqlResult, ThriftGen::Cell>
          cb(result, this, !noflush, !unbuffered);
      get_hql_interp(ns)->execute(hql, cb);
      //LOG_HQL_RESULT(result);
    } RETHROW("namespace=" << ns << " hql="<< hql <<" noflush="<< noflush
              << " unbuffered="<< unbuffered)

    LOG_API_FINISH;
  }

  virtual void
  hql_query(HqlResult& result, const ThriftGen::Namespace ns, const String &hql) {
    hql_exec(result, ns, hql, false, false);
  }

  virtual void
  hql_exec2(HqlResult2& result, const ThriftGen::Namespace ns, const String &hql, bool noflush,
            bool unbuffered) {
    LOG_API_START("namespace=" << ns << " hql="<< hql <<" noflush="<< noflush <<
                   " unbuffered="<< unbuffered);
    try {
      HqlCallback<HqlResult2, CellAsArray>
          cb(result, this, !noflush, !unbuffered);
      get_hql_interp(ns)->execute(hql, cb);
      //LOG_HQL_RESULT(result);
    } RETHROW("namespace=" << ns << " hql="<< hql <<" noflush="<< noflush <<
              " unbuffered="<< unbuffered)
    LOG_API_FINISH;
  }

  virtual void
  hql_query2(HqlResult2& result, const ThriftGen::Namespace ns, const String &hql) {
    hql_exec2(result, ns, hql, false, false);
  }

  virtual void create_namespace(const String &ns) {
    LOG_API_START("namespace=" << ns);
    try {
      m_client->create_namespace(ns, NULL);
    } RETHROW("namespace=" << ns)
    LOG_API_FINISH;
  }

  virtual void create_table(const ThriftGen::Namespace ns, const String &table, const String &schema) {
    LOG_API_START("namespace=" << ns << " table="<< table <<" schema="<< schema);

    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      namespace_ptr->create_table(table, schema);
    } RETHROW("namespace=" << ns << " table="<< table <<" schema="<< schema)

    LOG_API_FINISH;
  }

  virtual void alter_table(const ThriftGen::Namespace ns, const String &table, const String &schema) {
    LOG_API_START("namespace=" << ns << " table="<< table <<" schema="<< schema);

    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      namespace_ptr->alter_table(table, schema);
    } RETHROW("namespace=" << ns << " table="<< table <<" schema="<< schema)

    LOG_API_FINISH;
  }

  virtual Scanner
  open_scanner(const ThriftGen::Namespace ns, const String &table, const ThriftGen::ScanSpec &ss) {
    Scanner id;
    LOG_API_START("namespace=" << ns << " table="<< table <<" scan_spec="<< ss);
    try {
      id = get_scanner_id(_open_scanner(ns, table, ss).get());
    } RETHROW("namespace=" << ns << " table="<< table <<" scan_spec="<< ss)
    LOG_API_FINISH_E(" scanner="<<id);
    return id;
  }

  virtual ScannerAsync
  open_scanner_async(const ThriftGen::Namespace ns, const String &table,
                     const ThriftGen::Future ff,
                     const ThriftGen::ScanSpec &ss) {
    ScannerAsync id;
    LOG_API_START("namespace=" << ns << " table="<< table << " future=" << ff << " scan_spec="<< ss);
    try {
      id = get_scanner_async_id(_open_scanner_async(ns, table, ff, ss).get());
    } RETHROW("namespace=" << ns << " table="<< table << " future=" << ff << " scan_spec="<< ss)

    LOG_API_FINISH_E(" scanner=" << id);
    return id;
  }


  virtual void close_namespace(const ThriftGen::Namespace ns) {
    LOG_API_START("namespace="<< ns);
    try {
      remove_namespace_from_map(ns);
    } RETHROW("namespace="<< ns)
    LOG_API_FINISH;
  }

  virtual void close_scanner(const Scanner scanner) {
    LOG_API_START("scanner="<< scanner);
    try {
      remove_scanner(scanner);
    } RETHROW("scanner="<< scanner)
    LOG_API_FINISH;
  }

  virtual void cancel_scanner_async(const ScannerAsync scanner) {
    LOG_API_START("scanner="<< scanner);

    try {
      get_scanner_async(scanner)->cancel();
    } RETHROW(" scanner=" << scanner)

    LOG_API_FINISH_E(" cancelled");
  }


  virtual void close_scanner_async(const ScannerAsync scanner_async) {
    LOG_API_START("scanner_async="<< scanner_async);
    try {
      remove_scanner_async(scanner_async);
    } RETHROW("scanner_async="<< scanner_async)
    LOG_API_FINISH;
  }

  virtual void next_cells(ThriftCells &result, const Scanner scanner_id) {

    LOG_API_START("scanner="<< scanner_id);
    try {
      TableScannerPtr scanner = get_scanner(scanner_id);
      _next(result, scanner, m_next_threshold);
    } RETHROW("scanner="<< scanner_id)
    LOG_API_FINISH_E(" result.size=" << result.size());
  }

  virtual void
  next_cells_as_arrays(ThriftCellsAsArrays &result, const Scanner scanner_id) {

    LOG_API_START("scanner="<< scanner_id);
    try {
      TableScannerPtr scanner = get_scanner(scanner_id);
      _next(result, scanner, m_next_threshold);
    } RETHROW("scanner="<< scanner_id <<" result.size="<< result.size())
    LOG_API_FINISH_E("result.size="<< result.size());
  }

  virtual void
  next_cells_serialized(CellsSerialized& result, const Scanner scanner_id) {

    LOG_API_START("scanner="<< scanner_id);

    try {
      SerializedCellsWriter writer(m_next_threshold);
      Hypertable::Cell cell;

      TableScannerPtr scanner = get_scanner(scanner_id);

      while (1) {
        if (scanner->next(cell)) {
          if (!writer.add(cell)) {
            writer.finalize(SerializedCellsFlag::EOB);
            scanner->unget(cell);
            break;
          }
        }
        else {
          writer.finalize(SerializedCellsFlag::EOS);
          break;
        }
      }

      result = String((char *)writer.get_buffer(), writer.get_buffer_length());
    } RETHROW("scanner="<< scanner_id);
    LOG_API_FINISH_E("result.size="<< result.size());
  }


  virtual void next_row(ThriftCells &result, const Scanner scanner_id) {

    LOG_API_START("scanner="<< scanner_id <<" result.size="<< result.size());
    try {
      TableScannerPtr scanner = get_scanner(scanner_id);
      _next_row(result, scanner);
    } RETHROW("scanner=" << scanner_id)

    LOG_API_FINISH_E(" result.size="<< result.size());
  }

  virtual void
  next_row_as_arrays(ThriftCellsAsArrays &result, const Scanner scanner_id) {

    LOG_API_START("scanner="<< scanner_id);
    try {
      TableScannerPtr scanner = get_scanner(scanner_id);
      _next_row(result, scanner);
    } RETHROW(" result.size=" << result.size())
    LOG_API_FINISH_E(" result.size="<< result.size());
  }

  virtual void
  next_row_serialized(CellsSerialized& result, const Scanner scanner_id) {
    LOG_API_START("scanner="<< scanner_id);

    try {
      SerializedCellsWriter writer(0, true);
      Hypertable::Cell cell;
      std::string prev_row;

      TableScannerPtr scanner = get_scanner(scanner_id);

      while (1) {
        if (scanner->next(cell)) {
          // keep scanning
          if (prev_row.empty() || prev_row == cell.row_key) {
            // add cells from this row
            writer.add(cell);
            if (prev_row.empty())
              prev_row = cell.row_key;
          }
          else {
            // done with this row
            writer.finalize(SerializedCellsFlag::EOB);
            scanner->unget(cell);
            break;
          }
        }
        else {
          // done with this scan
          writer.finalize(SerializedCellsFlag::EOS);
          break;
        }
      }

      result = String((char *)writer.get_buffer(), writer.get_buffer_length());
    } RETHROW("scanner="<< scanner_id)
    LOG_API_FINISH_E(" result.size="<< result.size());
  }

  virtual void
  get_row(ThriftCells &result, const ThriftGen::Namespace ns, const String &table, const String &row) {
    LOG_API_START("namespace=" << ns << " table="<< table <<" row="<< row);
    try {
      _get_row(result,ns, table, row);
    } RETHROW("namespace=" << ns << " table="<< table <<" row="<< row)
    LOG_API_FINISH_E(" result.size="<< result.size());
  }

  virtual void
  get_row_as_arrays(ThriftCellsAsArrays &result, const ThriftGen::Namespace ns,
                    const String &table, const String &row) {
    LOG_API_START("namespace=" << ns << " table="<< table <<" row="<< row);

    try {
      _get_row(result, ns, table, row);
    } RETHROW("namespace=" << ns << " table="<< table <<" row="<< row)
    LOG_API_FINISH_E("result.size="<< result.size());
  }

  virtual void
  get_row_serialized(CellsSerialized& result,const ThriftGen::Namespace ns,
                     const std::string& table, const std::string& row) {
    LOG_API_START("namespace=" << ns << " table="<< table <<" row"<< row);

    try {
      SerializedCellsWriter writer(0, true);
      NamespacePtr namespace_ptr = get_namespace(ns);
      TablePtr t = namespace_ptr->open_table(table);
      Hypertable::ScanSpec ss;
      ss.row_intervals.push_back(Hypertable::RowInterval(row.c_str(), true,
                                                         row.c_str(), true));
      ss.max_versions = 1;
      TableScannerPtr scanner = t->create_scanner(ss);
      Hypertable::Cell cell;

      while (scanner->next(cell))
        writer.add(cell);
      writer.finalize(SerializedCellsFlag::EOS);

      result = String((char *)writer.get_buffer(), writer.get_buffer_length());
    } RETHROW("namespace=" << ns << " table="<< table <<" row"<< row)
    LOG_API_FINISH_E(" result.size="<< result.size());
  }

  virtual void
  get_cell(Value &result, const ThriftGen::Namespace ns, const String &table,
           const String &row, const String &column) {

    LOG_API_START("namespace=" << ns << " table="<< table <<" row="<< row <<" column="<< column);
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      TablePtr t = namespace_ptr->open_table(table);
      Hypertable::ScanSpec ss;

      ss.cell_intervals.push_back(Hypertable::CellInterval(row.c_str(),
          column.c_str(), true, row.c_str(), column.c_str(), true));
      ss.max_versions = 1;

      Hypertable::Cell cell;
      TableScannerPtr scanner = t->create_scanner(ss, 0, true);

      if (scanner->next(cell))
        result = String((char *)cell.value, cell.value_len);

    } RETHROW("namespace=" << ns << " table="<< table <<" row="<< row <<" column="<< column)
    LOG_API_FINISH_E(" result=" << result);
  }

  virtual void
  get_cells(ThriftCells &result, const ThriftGen::Namespace ns, const String &table,
            const ThriftGen::ScanSpec &ss) {
    LOG_API_START("namespace=" << ns << " table="<< table <<" scan_spec="<< ss<<" result.size="<< result.size());

    try {
      TableScannerPtr scanner = _open_scanner(ns, table, ss);
      _next(result, scanner, INT32_MAX);
    } RETHROW("namespace=" << ns << " table="<< table <<" scan_spec="<< ss)
    LOG_API_FINISH_E(" result.size="<< result.size());
  }

  virtual void
  get_cells_as_arrays(ThriftCellsAsArrays &result, const ThriftGen::Namespace ns, const String &table,
                      const ThriftGen::ScanSpec &ss) {
    LOG_API_START("namespace=" << ns << " table="<< table <<" scan_spec="<< ss);

    try {
      TableScannerPtr scanner = _open_scanner(ns, table, ss);
      _next(result, scanner, INT32_MAX);
    } RETHROW("namespace=" << ns << " table="<< table <<" scan_spec="<< ss)
    LOG_API_FINISH_E(" result.size="<< result.size());
  }

  virtual void
  get_cells_serialized(CellsSerialized& result, const ThriftGen::Namespace ns,
                       const String& table, const ThriftGen::ScanSpec& ss) {
    LOG_API_START("namespace=" << ns << " table="<< table <<" scan_spec="<< ss);

    try {
      SerializedCellsWriter writer(0, true);
      TableScannerPtr scanner = _open_scanner(ns, table, ss);
      Hypertable::Cell cell;

      while (scanner->next(cell))
        writer.add(cell);
      writer.finalize(SerializedCellsFlag::EOS);

      result = String((char *)writer.get_buffer(), writer.get_buffer_length());
    } RETHROW("namespace=" << ns << " table="<< table <<" scan_spec="<< ss)
    LOG_API_FINISH_E(" result.size="<< result.size());
  }

  virtual void
  offer_cells(const ThriftGen::Namespace ns, const String &table, const ThriftGen::MutateSpec &mutate_spec,
            const ThriftCells &cells) {

    LOG_API_START("namespace=" << ns << " table=" << table <<" mutate_spec.appname="<< mutate_spec.appname);

    try {
      _offer_cells(ns, table, mutate_spec, cells);
    } RETHROW("namespace=" << ns << " table=" << table <<" mutate_spec.appname="<< mutate_spec.appname)
    LOG_API_FINISH_E(" cells.size="<< cells.size());
  }

  virtual void
  offer_cell(const ThriftGen::Namespace ns, const String &table, const ThriftGen::MutateSpec &mutate_spec,
            const ThriftGen::Cell &cell) {

    LOG_API_START(" namespace=" << ns << " table=" << table <<" mutate_spec.appname="<< mutate_spec.appname);

    try {
      _offer_cell(ns, table, mutate_spec, cell);
    } RETHROW(" namespace=" << ns << " table=" << table <<" mutate_spec.appname="<< mutate_spec.appname)
    LOG_API_FINISH_E(" cell="<< cell);
  }

  virtual void
  offer_cells_as_arrays(const ThriftGen::Namespace ns, const String &table,
      const ThriftGen::MutateSpec &mutate_spec, const ThriftCellsAsArrays &cells) {

    LOG_API_START(" namespace=" << ns << " table=" << table << " mutate_spec.appname="<< mutate_spec.appname);

    try {
      _offer_cells(ns, table, mutate_spec, cells);
      LOG_API("mutate_spec.appname="<< mutate_spec.appname <<" done");
    } RETHROW(" namespace=" << ns << " table=" << table << " mutate_spec.appname="<< mutate_spec.appname)
    LOG_API_FINISH_E(" cells.size="<< cells.size());
  }

  virtual void
  offer_cell_as_array(const ThriftGen::Namespace ns, const String &table,
      const ThriftGen::MutateSpec &mutate_spec, const CellAsArray &cell) {
    // gcc 4.0.1 cannot seems to handle << cell here (see ThriftHelper.h)
    LOG_API_START("namespace=" << ns << " table=" << table << " mutate_spec.appname="<< mutate_spec.appname);

    try {
      _offer_cell(ns, table, mutate_spec, cell);
      LOG_API("mutate_spec.appname="<< mutate_spec.appname <<" done");
    } RETHROW("namespace=" << ns << " table=" << table << " mutate_spec.appname="<< mutate_spec.appname)
    LOG_API_FINISH_E(" cell.size="<< cell.size());
  }

  virtual ThriftGen::Future open_future(int queue_size) {
    ThriftGen::Future id;
    LOG_API_START("queue_size=" << queue_size);
    try {
      queue_size = (queue_size <= 0) ? m_future_queue_size : queue_size;
      FuturePtr future_ptr = new Hypertable::Future(queue_size);
      id = get_future_id(&future_ptr);
    } RETHROW("queue_size=" << queue_size)
    LOG_API_FINISH_E(" future=" << id);
    return id;
  }

  virtual void
  get_future_result(ThriftGen::Result &tresult, ThriftGen::Future ff) {

    LOG_API_START("future=" << ff);

    try {
      FuturePtr future_ptr = get_future(ff);
      ResultPtr hresult;
      bool done = !(future_ptr->get(hresult));
      if (done) {
        tresult.is_empty = true;
        tresult.id = 0;
        LOG_API_FINISH_E(" is_empty="<< tresult.is_empty);
      }
      else {
        tresult.is_empty = false;
        _convert_result(hresult, tresult);
        LOG_API_FINISH_E(" is_empty=" << tresult.is_empty << " id=" << tresult.id <<" is_scan="<<tresult.is_scan << " is_error=" << tresult.is_error);
      }
    } RETHROW("future=" << ff)
  }

  virtual void
  get_future_result_as_arrays(ThriftGen::ResultAsArrays &tresult, ThriftGen::Future ff) {

    LOG_API_START("future=" << ff);
    try {
      FuturePtr future_ptr = get_future(ff);
      ResultPtr hresult;
      bool done = !(future_ptr->get(hresult));
      if (done) {
        tresult.is_empty = true;
        tresult.id = 0;
        LOG_API_FINISH_E(" done="<< done );
      }
      else {
        tresult.is_empty = false;
        _convert_result_as_arrays(hresult, tresult);
        LOG_API_FINISH_E(" done=" << done << " id=" << tresult.id <<" is_scan="<<tresult.is_scan << "is_error=" << tresult.is_error);
      }
    } RETHROW("future=" << ff)
  }

  virtual void
  get_future_result_serialized(ThriftGen::ResultSerialized &tresult, ThriftGen::Future ff) {

    LOG_API_START("future=" << ff);

    try {
      FuturePtr future_ptr = get_future(ff);
      ResultPtr hresult;
      bool done = !(future_ptr->get(hresult));
      if (done) {
        tresult.is_empty = true;
        tresult.id = 0;
        LOG_API_FINISH_E(" done="<< done );
      }
      else {
        tresult.is_empty = false;
        _convert_result_serialized(hresult, tresult);
        LOG_API_FINISH_E(" done=" << done << " id=" << tresult.id
                       <<" is_scan="<<tresult.is_scan << "is_error=" << tresult.is_error);
      }
    } RETHROW("future=" << ff)
  }

  virtual void
  cancel_future(ThriftGen::Future ff) {
    LOG_API_START("future=" << ff);

    try {
      FuturePtr future_ptr = get_future(ff);
      future_ptr->cancel();
    } RETHROW("future=" << ff)
    LOG_API_FINISH;
  }

  virtual bool
  future_is_empty(ThriftGen::Future ff) {

    LOG_API_START("future=" << ff);
    bool is_empty;
    try {
      FuturePtr future_ptr = get_future(ff);
      is_empty = future_ptr->is_empty();
    } RETHROW("future=" << ff)
    LOG_API_FINISH_E(" is_empty=" << is_empty);
    return is_empty;
  }

  virtual bool
  future_is_full(ThriftGen::Future ff) {

    LOG_API_START("future=" << ff);
    bool full;
    try {
      FuturePtr future_ptr = get_future(ff);
      full = future_ptr->is_full();
    } RETHROW("future=" << ff)
    LOG_API_FINISH_E(" full=" << full);
    return full;
  }

  virtual bool
  future_is_cancelled(ThriftGen::Future ff) {
    LOG_API_START("future=" << ff);
    bool cancelled;
    try {
      FuturePtr future_ptr = get_future(ff);
      cancelled = future_ptr->is_cancelled();
    } RETHROW("future=" << ff)
    LOG_API_FINISH_E(" cancelled=" << cancelled);
    return cancelled;
  }

  virtual bool
  future_has_outstanding(ThriftGen::Future ff) {
    bool has_outstanding;
    LOG_API_START("future=" << ff);
    try {
      FuturePtr future_ptr = get_future(ff);
      has_outstanding = future_ptr->has_outstanding();
    } RETHROW("future=" << ff)
    LOG_API_FINISH_E(" has_outstanding=" << has_outstanding);
    return has_outstanding;
  }

  virtual void close_future(const ThriftGen::Future ff) {

    LOG_API_START("future="<< ff);
    try {
      remove_future_from_map(ff);
    } RETHROW(" future=" << ff)
    LOG_API_FINISH;
  }

  virtual ThriftGen::Namespace open_namespace(const String &ns) {
    ThriftGen::Namespace id;
    LOG_API_START("namespace name=" << ns);
    try {
      NamespacePtr namespace_ptr = m_client->open_namespace(ns);
      id = get_namespace_id(&namespace_ptr);
    } RETHROW(" namespace name" << ns)
    LOG_API_FINISH_E(" id=" << id);
    return id;
  }

  virtual MutatorAsync
  open_mutator_async(const ThriftGen::Namespace ns, const String &table,
                     const ThriftGen::Future ff, ::int32_t flags) {
    LOG_API_START("namespace=" << ns << " table="<< table << " future=" << ff <<" flags="<< flags);
    MutatorAsync id;
    try {
      id =  get_mutator_async_id(_open_mutator_async(ns, table, ff, flags).get());
    } RETHROW(" namespace=" << ns << " table="<< table << " future=" << ff <<" flags="<< flags)
    LOG_API_FINISH_E(" mutator=" << id);
    return id;
  }

  virtual Mutator open_mutator(const ThriftGen::Namespace ns, const String &table, ::int32_t flags,
                               ::int32_t flush_interval) {
    LOG_API_START("namespace=" << ns << "table="<< table <<" flags="<< flags << " flush_interval="<< flush_interval);
    Mutator id;
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      TablePtr t = namespace_ptr->open_table(table);
      id =  get_mutator_id(t->create_mutator(0, flags, flush_interval));
    } RETHROW(" namespace=" << ns << "table="<< table <<" flags="<< flags << " flush_interval="<< flush_interval)
    LOG_API_FINISH_E(" mutator_async=" << id);
    return id;
  }

  virtual void flush_mutator(const Mutator mutator) {
    LOG_API_START("mutator="<< mutator);
    try {
      get_mutator(mutator)->flush();
    } RETHROW(" mutator=" << mutator)
    LOG_API_FINISH_E(" done");
  }

  virtual void flush_mutator_async(const MutatorAsync mutator) {
    LOG_API_START("mutator="<< mutator);
    try {
      get_mutator_async(mutator)->flush();
    } RETHROW(" mutator=" << mutator)
    LOG_API_FINISH_E(" done");
  }


  virtual void close_mutator(const Mutator mutator) {

    LOG_API_START("mutator="<< mutator);
    try {
      flush_mutator(mutator);
      remove_mutator(mutator);
    } RETHROW(" mutator=" << mutator)
    LOG_API_FINISH;
  }

  virtual void cancel_mutator_async(const MutatorAsync mutator) {
    LOG_API_START("mutator="<< mutator);

    try {
      get_mutator_async(mutator)->cancel();
    } RETHROW(" mutator="<< mutator)

    LOG_API_FINISH_E(" cancelled");
  }

  virtual void close_mutator_async(const MutatorAsync mutator) {

    LOG_API_START("mutator="<< mutator);
    try {
      flush_mutator_async(mutator);
      remove_mutator_async(mutator);
    } RETHROW(" mutator" << mutator)
    LOG_API_FINISH;
  }

  virtual void set_cells(const Mutator mutator, const ThriftCells &cells) {

    LOG_API_START("mutator="<< mutator <<" cell.size="<< cells.size());
    try {
      _set_cells(mutator, cells);
    } RETHROW("mutator="<< mutator <<" cell.size="<< cells.size())
    LOG_API_FINISH;
  }

  virtual void set_cell(const Mutator mutator, const ThriftGen::Cell &cell) {

    LOG_API_START("mutator="<< mutator <<" cell="<< cell);
    try {
      _set_cell(mutator, cell);
    } RETHROW(" mutator="<< mutator <<" cell="<< cell)
    LOG_API_FINISH;
  }

  virtual void
  set_cells_as_arrays(const Mutator mutator, const ThriftCellsAsArrays &cells) {

    LOG_API_START("mutator="<< mutator <<" cell.size="<< cells.size());
    try {
      _set_cells(mutator, cells);
    } RETHROW(" mutator="<< mutator <<" cell.size="<< cells.size())
    LOG_API_FINISH;
  }

  virtual void
  set_cell_as_array(const Mutator mutator, const CellAsArray &cell) {
    // gcc 4.0.1 cannot seems to handle << cell here (see ThriftHelper.h)

    LOG_API_START("mutator="<< mutator <<" cell_as_array.size="<< cell.size());
    try {
      _set_cell(mutator, cell);
    } RETHROW(" mutator="<< mutator <<" cell_as_array.size="<< cell.size());
    LOG_API_FINISH;
  }

  virtual void
  set_cells_serialized(const Mutator mutator, const CellsSerialized &cells, const bool flush) {

    LOG_API_START("mutator="<< mutator <<" cell.size="<< cells.size());
    try {
      CellsBuilder cb;
      Hypertable::Cell hcell;
      SerializedCellsReader reader((void *)cells.c_str(), (uint32_t)cells.length());
      while (reader.next()) {
        reader.get(hcell);
        cb.add(hcell, false);
      }
	  get_mutator(mutator)->set_cells(cb.get());
      if (flush || reader.flush())
        get_mutator(mutator)->flush();
    } RETHROW(" mutator="<< mutator <<" cell.size="<< cells.size())

    LOG_API_FINISH;
  }

  virtual void set_cells_async(const MutatorAsync mutator, const ThriftCells &cells) {

    LOG_API_START("mutator="<< mutator <<" cell.size="<< cells.size());
    try {
      _set_cells_async(mutator, cells);
    } RETHROW(" mutator="<< mutator <<" cell.size="<< cells.size())
    LOG_API_FINISH;
  }

  virtual void set_cell_async(const MutatorAsync mutator, const ThriftGen::Cell &cell) {

    LOG_API_START("mutator="<< mutator <<" cell="<< cell);
    try {
      _set_cell_async(mutator, cell);
    } RETHROW(" mutator="<< mutator <<" cell="<< cell);
    LOG_API_FINISH;
  }

  virtual void
  set_cells_as_arrays_async(const MutatorAsync mutator, const ThriftCellsAsArrays &cells) {

    LOG_API_START("mutator="<< mutator <<" cell.size="<< cells.size());
    try {
      _set_cells_async(mutator, cells);
    } RETHROW(" mutator="<< mutator <<" cell.size="<< cells.size())
    LOG_API_FINISH;
  }

  virtual void
  set_cell_as_array_async(const MutatorAsync mutator, const CellAsArray &cell) {
    // gcc 4.0.1 cannot seems to handle << cell here (see ThriftHelper.h)
    LOG_API_START("mutator="<< mutator <<" cell_as_array.size="<< cell.size());
    try {
      _set_cell_async(mutator, cell);
    } RETHROW(" mutator="<< mutator <<" cell_as_array.size="<< cell.size())
    LOG_API_FINISH;
  }

  virtual void
  set_cells_serialized_async(const MutatorAsync mutator, const CellsSerialized &cells,
      const bool flush) {
   LOG_API_START("mutator="<< mutator <<" cell.size="<< cells.size());
    try {
      CellsBuilder cb;
      Hypertable::Cell hcell;
      SerializedCellsReader reader((void *)cells.c_str(), (uint32_t)cells.length());
      while (reader.next()) {
        reader.get(hcell);
        cb.add(hcell, false);
      }
      TableMutatorAsyncPtr mutator_ptr = get_mutator_async(mutator);
	    mutator_ptr->set_cells(cb.get());
      if (flush || reader.flush() || mutator_ptr->needs_flush())
        mutator_ptr->flush();

    } RETHROW(" mutator="<< mutator <<" cell.size="<< cells.size());
    LOG_API_FINISH;
  }

  virtual bool exists_namespace(const String &ns) {
    bool exists;
    LOG_API_START("namespace=" << ns);
    try {
      exists = m_client->exists_namespace(ns);
    } RETHROW(" namespace=" << ns)
    LOG_API_FINISH_E(" exists=" << exists);
    return exists;
  }

  virtual bool exists_table(const ThriftGen::Namespace ns, const String &table) {

    LOG_API_START("namespace=" << ns << " table="<< table);
    bool exists;
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      exists = namespace_ptr->exists_table(table);
    } RETHROW(" namespace=" << ns << " table="<< table)
    LOG_API_FINISH_E(" exists=" << exists);
    return exists;
  }

  virtual void get_table_id(String &result, const ThriftGen::Namespace ns, const String &table) {

    LOG_API_START("namespace=" << ns << " table="<< table);
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      result = namespace_ptr->get_table_id(table);
    } RETHROW(" namespace=" << ns << " table="<< table)
    LOG_API_FINISH_E(" id=" << result);
  }

  virtual void get_schema_str(String &result, const ThriftGen::Namespace ns, const String &table) {

    LOG_API_START("namespace=" << ns << " table="<< table);
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      result = namespace_ptr->get_schema_str(table);
    } RETHROW(" namespace=" << ns << " table="<< table)
    LOG_API_FINISH_E(" schema=" << result);
  }

  virtual void get_schema_str_with_ids(String &result, const ThriftGen::Namespace ns,
      const String &table) {

    LOG_API_START("namespace=" << ns << " table="<< table);
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      result = namespace_ptr->get_schema_str(table, true);
    } RETHROW(" namespace=" << ns << " table="<< table)
    LOG_API_FINISH_E(" schema=" << result);
  }


  virtual void get_schema(ThriftGen::Schema &result, const ThriftGen::Namespace ns, const String &table) {

    LOG_API_START("namespace=" << ns << " table="<< table);
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      Hypertable::SchemaPtr schema = namespace_ptr->get_schema(table);
      if (schema) {
        Hypertable::Schema::AccessGroups ags = schema->get_access_groups();
        foreach(Hypertable::Schema::AccessGroup *ag, ags) {
          ThriftGen::AccessGroup t_ag;

          t_ag.name = ag->name;
          t_ag.in_memory = ag->in_memory;
          t_ag.blocksize = (int32_t)ag->blocksize;
          t_ag.compressor = ag->compressor;
          t_ag.bloom_filter = ag->bloom_filter;

          foreach(Hypertable::Schema::ColumnFamily *cf, ag->columns) {
            ThriftGen::ColumnFamily t_cf;
            t_cf.name = cf->name;
            t_cf.ag = cf->ag;
            t_cf.max_versions = cf->max_versions;
            t_cf.ttl = (String) ctime(&(cf->ttl));
            t_cf.__isset.name = true;
            t_cf.__isset.ag = true;
            t_cf.__isset.max_versions = true;
            t_cf.__isset.ttl = true;

            // store this cf in the access group
            t_ag.columns.push_back(t_cf);
            // store this cf in the cf map
            result.column_families[t_cf.name] = t_cf;
          }
          t_ag.__isset.name = true;
          t_ag.__isset.in_memory = true;
          t_ag.__isset.blocksize = true;
          t_ag.__isset.compressor = true;
          t_ag.__isset.bloom_filter = true;
          t_ag.__isset.columns = true;
          // push this access group into the map
          result.access_groups[t_ag.name] = t_ag;
        }
        result.__isset.access_groups = true;
        result.__isset.column_families = true;
      }
    } RETHROW(" namespace=" << ns << " table="<< table)
    LOG_API_FINISH;
  }

  virtual void get_tables(std::vector<String> &tables, const ThriftGen::Namespace ns) {
    LOG_API_START("namespace=" << ns);
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      std::vector<Hypertable::NamespaceListing> listing;
      namespace_ptr->get_listing(false, listing);

      for(size_t ii=0; ii < listing.size(); ++ii)
        if (!listing[ii].is_namespace)
          tables.push_back(listing[ii].name);

    }
    RETHROW(" namespace=" << ns)
    LOG_API_FINISH_E(" tables.size=" << tables.size());
  }

  virtual void get_listing(std::vector<ThriftGen::NamespaceListing>& _return, const ThriftGen::Namespace ns) {

    LOG_API_START("namespace=" << ns);
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      std::vector<Hypertable::NamespaceListing> listing;
      namespace_ptr->get_listing(false, listing);
      ThriftGen::NamespaceListing entry;

      for(size_t ii=0; ii < listing.size(); ++ii) {
        entry.name = listing[ii].name;
        entry.is_namespace = listing[ii].is_namespace;
        _return.push_back(entry);
      }
    }
    RETHROW(" namespace=" << ns)

    LOG_API_FINISH_E(" listing.size="<< _return.size());
  }
  virtual void get_table_splits(std::vector<ThriftGen::TableSplit> & _return, const ThriftGen::Namespace ns,  const String &table) {

    TableSplitsContainer splits;
    LOG_API_START("namespace=" << ns << " table="<< table<< " splits.size="<< _return.size());
    try {
      ThriftGen::TableSplit tsplit;
      NamespacePtr namespace_ptr = get_namespace(ns);
      namespace_ptr->get_table_splits(table, splits);
      for (TableSplitsContainer::iterator iter=splits.begin(); iter != splits.end(); ++iter) {
	      convert_table_split(*iter, tsplit);
	      _return.push_back(tsplit);
      }
    }
    RETHROW(" namespace=" << ns << " table="<< table)

    LOG_API_FINISH_E(" splits.size=" << splits.size());
  }

  virtual void drop_namespace(const String &ns, const bool if_exists) {

    LOG_API_START("namespace=" << ns << " if_exists="<< if_exists);
    try {
      m_client->drop_namespace(ns, NULL, if_exists);
    }
    RETHROW(" namespace=" << ns << " if_exists="<< if_exists)
    LOG_API_FINISH;
  }

  virtual void rename_table(const ThriftGen::Namespace ns, const String &table,
                            const String &new_table_name) {
    LOG_API_START("namespace=" << ns << " table="<< table << " new_table_name=" << new_table_name << " done");

    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      namespace_ptr->rename_table(table, new_table_name);
    }
    RETHROW(" namespace=" << ns << " table="<< table << " new_table_name=" << new_table_name << " done")
    LOG_API_FINISH;
  }

  virtual void drop_table(const ThriftGen::Namespace ns, const String &table, const bool if_exists) {
    LOG_API_START("namespace=" << ns << " table="<< table <<" if_exists="<< if_exists);
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      namespace_ptr->drop_table(table, if_exists);
    }
    RETHROW("namespace=" << ns << " table="<< table <<" if_exists="<< if_exists)
    LOG_API_FINISH;
  }

  virtual void generate_guid(std::string& _return) {
    LOG_API_START("");
    try {
      _return=HyperAppHelper::generate_guid();
    }
    RETHROW("")
    LOG_API_FINISH;
  }

  virtual void create_cell_unique(std::string &_return, 
          const ThriftGen::Namespace ns, const std::string& table_name, 
          const ThriftGen::Key& tkey, const std::string& value) {
    LOG_API_START("namespace=" << ns << " table=" << table_name 
            << tkey << " value=" << value);
    std::string guid;
    try {
      NamespacePtr namespace_ptr = get_namespace(ns);
      Hypertable::KeySpec hkey;
      convert_key(tkey, hkey);
      TablePtr t = namespace_ptr->open_table(table_name);
      HyperAppHelper::create_cell_unique(t, hkey, 
              value.empty() ? guid : (std::string &)value);
    }
    RETHROW("namespace=" << ns << " table=" << table_name 
            << tkey << " value=" << value);
    LOG_API_FINISH;
    _return=value.empty() ? guid : value;
  }

  // helper methods
  void _convert_result(const Hypertable::ResultPtr &hresult, ThriftGen::Result &tresult) {
  Hypertable::Cells hcells;

    if (hresult->is_scan()) {
      tresult.is_scan = true;
      tresult.id = get_scanner_async_id(hresult->get_scanner());
      if (hresult->is_error()) {
        tresult.is_error = true;
        hresult->get_error(tresult.error, tresult.error_msg);
        tresult.__isset.error = true;
        tresult.__isset.error_msg = true;
      }
      else {
        tresult.is_error = false;
        tresult.__isset.cells = true;
        hresult->get_cells(hcells);
        convert_cells(hcells, tresult.cells);
      }
    }
    else {
      tresult.is_scan = false;
      tresult.id = get_mutator_async_id(hresult->get_mutator());
      if (hresult->is_error()) {
        tresult.is_error = true;
        hresult->get_error(tresult.error, tresult.error_msg);
        hresult->get_failed_cells(hcells);
        convert_cells(hcells, tresult.cells);
        tresult.__isset.error = true;
        tresult.__isset.error_msg = true;
      }
    }
  }

  void _convert_result_as_arrays(const Hypertable::ResultPtr &hresult,
      ThriftGen::ResultAsArrays &tresult) {
  Hypertable::Cells hcells;

    if (hresult->is_scan()) {
      tresult.is_scan = true;
      tresult.id = get_scanner_async_id(hresult->get_scanner());
      if (hresult->is_error()) {
        tresult.is_error = true;
        hresult->get_error(tresult.error, tresult.error_msg);
        tresult.__isset.error = true;
        tresult.__isset.error_msg = true;
      }
      else {
        tresult.is_error = false;
        tresult.__isset.cells = true;
        hresult->get_cells(hcells);
        convert_cells(hcells, tresult.cells);
      }
    }
    else {
      HT_THROW(Error::NOT_IMPLEMENTED, "Support for asynchronous mutators not yet implemented");
    }
  }

  void _convert_result_serialized(Hypertable::ResultPtr &hresult,
                                  ThriftGen::ResultSerialized &tresult) {
  Hypertable::Cells hcells;

    if (hresult->is_scan()) {
      tresult.is_scan = true;
      tresult.id = get_scanner_async_id(hresult->get_scanner());
      if (hresult->is_error()) {
        tresult.is_error = true;
        hresult->get_error(tresult.error, tresult.error_msg);
        tresult.__isset.error = true;
        tresult.__isset.error_msg = true;
      }
      else {
        tresult.is_error = false;
        tresult.__isset.cells = true;
        hresult->get_cells(hcells);
        convert_cells(hcells, tresult.cells);
      }
    }
    else {
      HT_THROW(Error::NOT_IMPLEMENTED, "Support for asynchronous mutators not yet implemented");
    }
  }

  TableMutatorAsyncPtr
  _open_mutator_async(const ThriftGen::Namespace ns, const String &table,
                      const ThriftGen::Future ff, ::int32_t flags) {
    NamespacePtr namespace_ptr = get_namespace(ns);
    TablePtr t = namespace_ptr->open_table(table);
    FuturePtr future_ptr = get_future(ff);

    return t->create_mutator_async(future_ptr.get(), flags);
  }

  TableScannerAsyncPtr
  _open_scanner_async(const ThriftGen::Namespace ns, const String &table,
                      const ThriftGen::Future ff, const ThriftGen::ScanSpec &ss) {
    NamespacePtr namespace_ptr = get_namespace(ns);
    TablePtr t = namespace_ptr->open_table(table);
    FuturePtr future_ptr = get_future(ff);

    Hypertable::ScanSpec hss;
    convert_scan_spec(ss, hss);
    return t->create_scanner_async(future_ptr.get(), hss, 0);
  }

  TableScannerPtr
  _open_scanner(const ThriftGen::Namespace ns, const String &table, const ThriftGen::ScanSpec &ss) {
    NamespacePtr namespace_ptr = get_namespace(ns);
    TablePtr t = namespace_ptr->open_table(table);
    Hypertable::ScanSpec hss;
    convert_scan_spec(ss, hss);
    return t->create_scanner(hss, 0);
  }

  template <class CellT>
  void _next(vector<CellT> &result, TableScannerPtr &scanner, int limit) {
    Hypertable::Cell cell;
    int32_t amount_read = 0;

    while (amount_read < limit) {
      if (scanner->next(cell)) {
        CellT tcell;
        amount_read += convert_cell(cell, tcell);
        result.push_back(tcell);
      }
      else
        break;
    }
  }

  template <class CellT>
  void _next_row(vector<CellT> &result, TableScannerPtr &scanner) {
    Hypertable::Cell cell;
    std::string prev_row;

    while (scanner->next(cell)) {
      if (prev_row.empty() || prev_row == cell.row_key) {
        CellT tcell;
        convert_cell(cell, tcell);
        result.push_back(tcell);
        if (prev_row.empty())
          prev_row = cell.row_key;
      }
      else {
        scanner->unget(cell);
        break;
      }
    }
  }

  template <class CellT>
  void _get_row(vector<CellT> &result, const ThriftGen::Namespace ns, const String &table,
      const String &row) {
    NamespacePtr namespace_ptr = get_namespace(ns);
    TablePtr t = namespace_ptr->open_table(table);
    Hypertable::ScanSpec ss;
    ss.row_intervals.push_back(Hypertable::RowInterval(row.c_str(), true,
                                                       row.c_str(), true));
    ss.max_versions = 1;
    TableScannerPtr scanner = t->create_scanner(ss);
    _next(result, scanner, INT32_MAX);
  }
  HqlInterpreterPtr &get_hql_interp(const ThriftGen::Namespace ns) {
    ScopedLock lock(m_interp_mutex);
    HqlInterpreterMap::iterator iter = m_hql_interp_map.find(ns);

    if (iter == m_hql_interp_map.end()){
      NamespacePtr namespace_ptr = get_namespace(ns);
      HqlInterpreterPtr interp = m_client->create_hql_interpreter(true);
      interp->set_namespace(namespace_ptr->get_name());
      m_hql_interp_map[ns] = interp;
    }

    return m_hql_interp_map.find(ns)->second;
  }

  template <class CellT>
  void _offer_cells(const ThriftGen::Namespace ns, const String &table,
                  const ThriftGen::MutateSpec &mutate_spec, const vector<CellT> &cells) {
    Hypertable::Cells hcells;
    convert_cells(cells, hcells);
    get_shared_mutator(ns, table, mutate_spec)->set_cells(hcells);
  }

  template <class CellT>
  void _offer_cell(const ThriftGen::Namespace ns, const String &table,
                 const ThriftGen::MutateSpec &mutate_spec, const CellT &cell) {
    CellsBuilder cb;
    Hypertable::Cell hcell;
    convert_cell(cell, hcell);
    cb.add(hcell, false);
    get_shared_mutator(ns, table, mutate_spec)->set_cells(cb.get());
  }

  template <class CellT>
  void _set_cells(const Mutator mutator, const vector<CellT> &cells) {
    Hypertable::Cells hcells;
    convert_cells(cells, hcells);
    get_mutator(mutator)->set_cells(hcells);
  }

  template <class CellT>
  void _set_cell(const Mutator mutator, const CellT &cell) {
    CellsBuilder cb;
    Hypertable::Cell hcell;
    convert_cell(cell, hcell);
    cb.add(hcell, false);
    get_mutator(mutator)->set_cells(cb.get());
  }

  template <class CellT>
  void _set_cells_async(const MutatorAsync mutator, const vector<CellT> &cells) {
    Hypertable::Cells hcells;
    convert_cells(cells, hcells);
    TableMutatorAsyncPtr mutator_ptr = get_mutator_async(mutator);
    mutator_ptr->set_cells(hcells);
    if (mutator_ptr->needs_flush())
      mutator_ptr->flush();
  }

  template <class CellT>
  void _set_cell_async(const MutatorAsync mutator, const CellT &cell) {
    CellsBuilder cb;
    Hypertable::Cell hcell;
    convert_cell(cell, hcell);
    cb.add(hcell, false);
    TableMutatorAsyncPtr mutator_ptr = get_mutator_async(mutator);
    mutator_ptr->set_cells(cb.get());
    if (mutator_ptr->needs_flush())
      mutator_ptr->flush();
  }

  FuturePtr get_future(::int64_t id) {
    ScopedLock lock(m_future_mutex);
    FutureMap::iterator it = m_future_map.find(id);

    if (it != m_future_map.end())
      return it->second;

    HT_ERROR_OUT << "Bad future id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_FUTURE_ID,
             format("Invalid future id: %lld", (Lld)id));

  }


  NamespacePtr get_namespace(::int64_t id) {
    ScopedLock lock(m_namespace_mutex);
    NamespaceMap::iterator it = m_namespace_map.find(id);

    if (it != m_namespace_map.end())
      return it->second;

    HT_ERROR_OUT << "Bad namespace id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_NAMESPACE_ID,
             format("Invalid namespace id: %lld", (Lld)id));

  }

  // returned id is guaranteed to be unique and non-zero
  ::int64_t get_future_id(FuturePtr *ff) {
    ScopedLock lock(m_future_mutex);
    ::int64_t id = m_next_future_id;
    m_next_future_id++;
    m_future_map.insert(make_pair(id, *ff)); // no overwrite
    return id;
  }


  // returned id is guaranteed to be unique and non-zero
  ::int64_t get_namespace_id(NamespacePtr *ns) {
    ScopedLock lock(m_namespace_mutex);
    // generate unique random 64 bit int id
    // TODO make id random for security reasons
    //::int64_t id = Random::number64();
    ::int64_t id = m_next_namespace_id++;

    // TODO make id random for security reasons
    //while (m_namespace_map.find(id) != m_namespace_map.end() || id == 0) {
    //  id = Random::number64();
    //}
    m_namespace_map.insert(make_pair(id, *ns)); // no overwrite
    return id;
  }

  ::int64_t get_scanner_async_id(TableScannerAsync *scanner) {
    ScopedLock lock(m_scanner_async_mutex);
    ::int64_t id = (::int64_t)scanner;

    // if scanner is already in map then return id by reverse lookup
    ReverseScannerAsyncMap::iterator it =
        m_reverse_scanner_async_map.find((::int64_t)scanner);
    if (it != m_reverse_scanner_async_map.end())
      id = it->second;
    else {
      m_scanner_async_map.insert(make_pair(id, scanner)); // no overwrite
      m_reverse_scanner_async_map.insert(make_pair((::int64_t)scanner, id));
    }
    return id;
  }

  TableScannerAsyncPtr get_scanner_async(::int64_t id) {
    ScopedLock lock(m_scanner_async_mutex);
    ScannerAsyncMap::iterator it = m_scanner_async_map.find(id);

    if (it != m_scanner_async_map.end())
      return it->second;

    HT_ERROR_OUT << "Bad scanner id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_SCANNER_ID,
             format("Invalid scanner id: %lld", (Lld)id));
  }

  ::int64_t get_scanner_id(TableScanner *scanner) {
    ScopedLock lock(m_scanner_mutex);
    ::int64_t id = (::int64_t)scanner;
    m_scanner_map.insert(make_pair(id, scanner)); // no overwrite
    return id;
  }

  TableScannerPtr get_scanner(::int64_t id) {
    ScopedLock lock(m_scanner_mutex);
    ScannerMap::iterator it = m_scanner_map.find(id);

    if (it != m_scanner_map.end())
      return it->second;

    HT_ERROR_OUT << "Bad scanner id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_SCANNER_ID,
             format("Invalid scanner id: %lld", (Lld)id));
  }

  void remove_scanner(::int64_t id) {
    ScopedLock lock(m_scanner_mutex);
    ScannerMap::iterator it = m_scanner_map.find(id);

    if (it != m_scanner_map.end()) {
      m_scanner_map.erase(it);
      return;
    }

    HT_ERROR_OUT << "Bad scanner id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_SCANNER_ID,
             format("Invalid scanner id: %lld", (Lld)id));
  }

  void remove_scanner_async(::int64_t id) {
    ScopedLock lock(m_scanner_async_mutex);
    ScannerAsyncMap::iterator it = m_scanner_async_map.find(id);
    ::int64_t scanner_async=0;

    if (it != m_scanner_async_map.end()) {
      scanner_async = (::int64_t)(it->second.get());
      m_scanner_async_map.erase(it);
      m_reverse_scanner_async_map.erase(scanner_async);
      return;
    }

    HT_ERROR_OUT << "Bad scanner id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_SCANNER_ID,
             format("Invalid scanner id: %lld", (Lld)id));
  }

  ::int64_t get_mutator_id(TableMutator *mutator) {
    ScopedLock lock(m_mutator_mutex);
    ::int64_t id = (::int64_t)mutator;
    m_mutator_map.insert(make_pair(id, mutator)); // no overwrite
    return id;
  }

  ::int64_t get_mutator_async_id(TableMutatorAsync *mutator) {
    ScopedLock lock(m_mutator_async_mutex);
    ::int64_t id = (::int64_t)mutator;
    m_mutator_async_map.insert(make_pair(id, mutator)); // no overwrite
    return id;
  }


  virtual void refresh_shared_mutator(const ThriftGen::Namespace ns, const String &table,
      const ThriftGen::MutateSpec &mutate_spec) {
    ScopedLock lock(m_shared_mutator_mutex);
    SharedMutatorMapKey skey(ns, table, mutate_spec);

    SharedMutatorMap::iterator it = m_shared_mutator_map.find(skey);

    // if mutator exists then delete it
    if (it != m_shared_mutator_map.end()) {
      LOG_API("deleting shared mutator on namespace=" << ns << " table=" << table <<
              " with appname=" << mutate_spec.appname);
      m_shared_mutator_map.erase(it);
    }

    //re-create the shared mutator
    // else create it and insert it in the map
    LOG_API("creating shared mutator on namespace=" << ns << " table="<< table
            <<" with appname=" << mutate_spec.appname);
    NamespacePtr namespace_ptr = get_namespace(ns);
    TablePtr t = namespace_ptr->open_table(table);
    TableMutatorPtr mutator = t->create_mutator(0, mutate_spec.flags, mutate_spec.flush_interval);
    m_shared_mutator_map[skey] = mutator;
    return;
  }


  TableMutatorPtr get_shared_mutator(const ThriftGen::Namespace ns, const String &table,
                                     const ThriftGen::MutateSpec &mutate_spec) {
    ScopedLock lock(m_shared_mutator_mutex);
    SharedMutatorMapKey skey(ns, table, mutate_spec);

    SharedMutatorMap::iterator it = m_shared_mutator_map.find(skey);

    // if mutator exists then return it
    if (it != m_shared_mutator_map.end())
      return it->second;
    else {
      // else create it and insert it in the map
      LOG_API("creating shared mutator on namespace="<< ns << " table="<< table <<
              " with appname=" << mutate_spec.appname);
      NamespacePtr namespace_ptr = get_namespace(ns);
      TablePtr t = namespace_ptr->open_table(table);
      TableMutatorPtr mutator = t->create_mutator(0, mutate_spec.flags, mutate_spec.flush_interval);
      m_shared_mutator_map[skey] = mutator;
      return mutator;
    }
  }

  TableMutatorPtr get_mutator(::int64_t id) {
    ScopedLock lock(m_mutator_mutex);
    MutatorMap::iterator it = m_mutator_map.find(id);

    if (it != m_mutator_map.end())
      return it->second;

    HT_ERROR_OUT << "Bad mutator id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_MUTATOR_ID,
             format("Invalid mutator id: %lld", (Lld)id));
  }

  TableMutatorAsyncPtr get_mutator_async(::int64_t id) {
    ScopedLock lock(m_mutator_async_mutex);
    MutatorAsyncMap::iterator it = m_mutator_async_map.find(id);

    if (it != m_mutator_async_map.end())
      return it->second;

    HT_ERROR_OUT << "Bad mutator id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_MUTATOR_ID,
             format("Invalid mutator id: %lld", (Lld)id));
  }

  void remove_future_from_map(::int64_t id) {
    ScopedLock lock(m_future_mutex);
    FutureMap::iterator it = m_future_map.find(id);

    if (it != m_future_map.end()) {
      m_future_map.erase(it);
      return;
    }

    HT_ERROR_OUT << "Bad future id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_FUTURE_ID,
             format("Invalid future id: %lld", (Lld)id));
  }

  void remove_namespace_from_map(::int64_t id) {
    ScopedLock lock(m_namespace_mutex);
    NamespaceMap::iterator it = m_namespace_map.find(id);

    if (it != m_namespace_map.end()) {
      m_namespace_map.erase(it);
      return;
    }

    HT_ERROR_OUT << "Bad namespace id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_NAMESPACE_ID,
             format("Invalid namespace id: %lld", (Lld)id));
  }


  void remove_mutator(::int64_t id) {
    ScopedLock lock(m_mutator_mutex);
    MutatorMap::iterator it = m_mutator_map.find(id);

    if (it != m_mutator_map.end()) {
      m_mutator_map.erase(it);
      return;
    }

    HT_ERROR_OUT << "Bad mutator id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_MUTATOR_ID,
             format("Invalid mutator id: %lld", (Lld)id));
  }

  void remove_mutator_async(::int64_t id) {
    ScopedLock lock(m_mutator_async_mutex);
    MutatorAsyncMap::iterator it = m_mutator_async_map.find(id);

    if (it != m_mutator_async_map.end()) {
      m_mutator_async_map.erase(it);
      return;
    }

    HT_ERROR_OUT << "Bad mutator id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_MUTATOR_ID,
             format("Invalid mutator id: %lld", (Lld)id));
  }

private:
  bool             m_log_api;
  Mutex            m_scanner_mutex;
  ScannerMap       m_scanner_map;
  Mutex            m_mutator_mutex;
  Mutex            m_mutator_async_mutex;
  MutatorMap       m_mutator_map;
  MutatorAsyncMap  m_mutator_async_map;
  Mutex            m_shared_mutator_mutex;
  ::int64_t        m_next_namespace_id;
  NamespaceMap     m_namespace_map;
  Mutex            m_namespace_mutex;
  ScannerAsyncMap  m_scanner_async_map;
  ReverseScannerAsyncMap m_reverse_scanner_async_map;
  Mutex            m_scanner_async_mutex;
  ::int64_t        m_next_future_id;
  FutureMap        m_future_map;
  Mutex            m_future_mutex;
  ::int32_t        m_future_queue_size;
  SharedMutatorMap m_shared_mutator_map;
  ::int32_t        m_next_threshold;
  ClientPtr        m_client;
  Mutex            m_interp_mutex;
  HqlInterpreterMap m_hql_interp_map;
};

template <class ResultT, class CellT>
void HqlCallback<ResultT, CellT>::on_return(const String &ret) {
  result.results.push_back(ret);
  result.__isset.results = true;
}

template <class ResultT, class CellT>
void HqlCallback<ResultT, CellT>::on_scan(TableScanner &s) {
  if (buffered) {
    Hypertable::Cell hcell;
    CellT tcell;

    while (s.next(hcell)) {
      convert_cell(hcell, tcell);
      result.cells.push_back(tcell);
    }
    result.__isset.cells = true;
  }
  else {
    result.scanner = handler.get_scanner_id(&s);
    result.__isset.scanner = true;
  }
}

template <class ResultT, class CellT>
void HqlCallback<ResultT, CellT>::on_finish(TableMutator *m) {
  if (flush) {
    Parent::on_finish(m);
  }
  else if (m) {
    result.mutator = handler.get_mutator_id(m);
    result.__isset.mutator = true;
  }
}

}} // namespace Hypertable::ThriftBroker


int main(int argc, char **argv) {
  using namespace Hypertable;
  using namespace ThriftBroker;
  Random::seed(time(NULL));

  try {
    init_with_policies<Policies>(argc, argv);

    if (get_bool("ThriftBroker.Hyperspace.Session.Reconnect"))
      properties->set("Hyperspace.Session.Reconnect", true);

    ::uint16_t port = get_i16("port");
    boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    boost::shared_ptr<ServerHandler> handler(new ServerHandler());
    boost::shared_ptr<TProcessor> processor(new HqlServiceProcessor(handler));

    boost::shared_ptr<TServerTransport> serverTransport;

    if (has("thrift-timeout")) {
      int timeout_ms = get_i32("thrift-timeout");
      serverTransport.reset( new TServerSocket(port, timeout_ms, timeout_ms) );
    }
    else
      serverTransport.reset( new TServerSocket(port) );

    boost::shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());

    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);

    HT_INFO("Starting the server...");
    server.serve();
    HT_INFO("Exiting.\n");
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
  return 0;
}
