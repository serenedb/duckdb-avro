#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct AvroMetadata {
	static TableFunction GetFunction();
};

} // namespace duckdb
