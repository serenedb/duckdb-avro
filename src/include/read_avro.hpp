//===----------------------------------------------------------------------===//
//                         DuckDB
//
// read_avro.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct ReadAvro {
	static TableFunction GetSingleFileFunction();
	static TableFunctionMultiFileSettings GetMultiFileSettings();
	static TableFunctionSet GetFunctionSet();
	static unique_ptr<TableRef> Replacement(ClientContext &context, ReplacementScanInput &input,
	                                        optional_ptr<ReplacementScanData> data);
};

} // namespace duckdb
