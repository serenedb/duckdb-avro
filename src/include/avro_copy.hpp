#pragma once

#include "duckdb/function/copy_function.hpp"

namespace duckdb {

struct AvroCopyFunction {
	static CopyFunction Create();
};

} // namespace duckdb
