//===----------------------------------------------------------------------===//
//                         DuckDB
//
// avro_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "avro_binary.hpp"
#include "avro_type.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/formats/row_input.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_state.hpp"

#include <span>

namespace duckdb {

class AvroSource {
public:
	AvroSource(ClientContext &context, OpenFileInfo file);

	const string &Path() const {
		return file.path;
	}
	RowSource &Rows() const {
		return input->Rows();
	}
	idx_t Size() const {
		return input->size;
	}
	bool Stream() const {
		return input->stream;
	}
	void ReadInOrder();
	void Consume(idx_t bytes) {
		consumed += bytes;
	}
	double Progress() const;

public:
	OpenFileInfo file;
	AvroHeader header;

private:
	shared_ptr<CSVBufferManager> manager;
	unique_ptr<RowInput> input;
	atomic<idx_t> consumed {0};
};

enum class AvroDecode : uint8_t {
	NULL_VALUE,
	BOOLEAN,
	INT,
	DATE,
	TIME_MILLIS,
	TIME_MICROS,
	LONG,
	TIMESTAMP_MILLIS,
	FLOAT,
	DOUBLE,
	STRING,
	UUID_STRING,
	BYTES,
	DECIMAL_BYTES,
	FIXED,
	UUID_FIXED,
	DECIMAL_FIXED,
	DURATION,
	ENUM,
	RECORD,
	EMPTY_RECORD,
	ARRAY,
	MAP,
	UNION,
	NULLABLE
};

enum class AvroFast : uint8_t {
	NONE,
	BOOLEAN,
	INT,
	TIME_MILLIS,
	TIME_MICROS,
	LONG,
	TIMESTAMP_MILLIS,
	FLOAT,
	DOUBLE,
	STRING,
	BYTES
};

struct AvroNode {
	static AvroNode Plan(const AvroType &type);

	AvroDecode decode = AvroDecode::NULL_VALUE;
	AvroKind kind = AvroKind::NULL_TYPE;
	AvroFast fast = AvroFast::NONE;
	uint8_t null_byte = 0xFF;
	uint8_t value_byte = 0xFF;
	PhysicalType physical = PhysicalType::INVALID;
	idx_t size = 0;
	bool strings = false;
	vector<AvroNode> children;
	vector<idx_t> members;
};

enum class AvroFieldRole : uint8_t { SKIP, DECODE, DEFER };

struct AvroField {
	AvroNode node;
	AvroFieldRole role = AvroFieldRole::SKIP;
	idx_t output = DConstants::INVALID_INDEX;
	idx_t deferred = DConstants::INVALID_INDEX;
};

struct AvroTarget {
	Vector *vector = nullptr;
	data_ptr_t data = nullptr;
	ValidityMask *validity = nullptr;
};

struct AvroReaderFilter {
	idx_t output_column;
	reference<const TableFilter> filter;
};

struct AvroReaderColumn {
	idx_t file_column;
	idx_t output_column;
};

struct AvroReaderColumns {
	vector<AvroReaderColumn> reads;
	vector<AvroReaderFilter> filters;
	idx_t file_row_number = DConstants::INVALID_INDEX;
};

class AvroReaderGlobal {
public:
	AvroReaderGlobal(ClientContext &context, shared_ptr<AvroSource> source, const AvroType &root,
	                 AvroReaderColumns columns);

	idx_t MaxThreads() const;
	bool Claim(idx_t &range) {
		return ranges->Claim(range);
	}
	void Finish(idx_t range) {
		ranges->Finish(range);
	}
	double Progress() const {
		return source->Progress();
	}

public:
	ClientContext &context;
	shared_ptr<AvroSource> source;
	AvroReaderColumns columns;
	vector<AvroField> fields;
	unique_ptr<AvroNode> root_union;
	idx_t record_branch = 0;
	unique_ptr<RowRanges> ranges;
	idx_t deferred = 0;
	bool walk = false;
};

class AvroReader {
public:
	explicit AvroReader(AvroReaderGlobal &global);

	bool Claim();
	bool Claimed() const {
		return claimed;
	}
	void Release() {
		claimed = false;
	}
	idx_t Scan(DataChunk &output);
	idx_t Lookup(std::span<const int64_t> rows, std::span<idx_t> survivors, DataChunk &output);

private:
	void Load();
	bool NextBlock();
	bool FindSync(idx_t &position);
	idx_t Ensure(idx_t from, idx_t to);
	idx_t KeepFrom(idx_t start) const {
		return searching ? MinValue(sync_from, start) : start;
	}
	const char *At(idx_t position) const {
		return window.bytes->data + (position - window.bytes->offset);
	}
	bool ParseBlock(idx_t start, int64_t &count, idx_t &data_start, idx_t &size);
	void OpenBlock(idx_t data_start, idx_t size);
	shared_ptr<RowBytes> PoolBuffer();
	bool LookupBlock(RowLookup &lookup, idx_t block, idx_t &records);
	void Pin(const shared_ptr<RowBytes> &bytes);
	void Target(DataChunk &output, const AvroField &field);
	idx_t Fill(DataChunk &output);
	void Walk(idx_t first, idx_t count, DataChunk &output);
	bool RootBranch(const char *&pos, const char *end);
	idx_t Complete(DataChunk &output, idx_t rows);
	idx_t ApplyFilters(DataChunk &output, idx_t rows, bool lookup);
	[[noreturn]] void ThrowCorrupt(const string &detail) const;

private:
	AvroReaderGlobal &global;
	const AvroHeader &header;
	RowWindow window;
	AvroDecompressor decompressor;
	vector<unique_ptr<TableFilterState>> filter_states;
	SelectionVector filter_selection;
	idx_t range = 0;
	idx_t range_end = 0;
	idx_t next_block = 0;
	idx_t sync_from = 0;
	bool claimed = false;
	bool loaded = false;
	bool searching = false;
	idx_t block_offset = 0;
	idx_t block_left = 0;
	idx_t record_index = 0;
	const char *cursor = nullptr;
	const char *block_end = nullptr;
	shared_ptr<RowBytes> block_bytes;
	vector<shared_ptr<RowBytes>> pins;
	vector<shared_ptr<RowBytes>> pool;
	vector<AvroTarget> targets;
	vector<const char *> positions;
	const char *row_ends[STANDARD_VECTOR_SIZE];
	int64_t identities[STANDARD_VECTOR_SIZE];
};

} // namespace duckdb
