//===----------------------------------------------------------------------===//
//                         DuckDB
//
// avro_binary.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

#define ZLIB_CONST
#include <zlib.h>
#include <zstd.h>

#include <string_view>

namespace duckdb {

static constexpr idx_t AVRO_SYNC_SIZE = 16;
static constexpr idx_t AVRO_MAGIC_SIZE = 4;
static constexpr char AVRO_MAGIC[AVRO_MAGIC_SIZE] = {'O', 'b', 'j', 1};
static constexpr idx_t AVRO_MAX_VARINT_SIZE = 10;
static constexpr idx_t AVRO_PADDING = 64;

enum class AvroCodec : uint8_t { NONE, DEFLATE, SNAPPY, ZSTANDARD };

AvroCodec AvroCodecFromName(std::string_view name);
std::string_view AvroCodecName(AvroCodec codec);

inline bool AvroReadVarint(const char *&pos, uint64_t &value) {
	auto bytes = reinterpret_cast<const uint8_t *>(pos);
	uint64_t byte = bytes[0];
	if (byte < 0x80) {
		value = byte;
		pos++;
		return true;
	}
	uint64_t result = byte & 0x7F;
	for (idx_t i = 1; i < AVRO_MAX_VARINT_SIZE; i++) {
		byte = bytes[i];
		result |= (byte & 0x7F) << (7 * i);
		if (byte < 0x80) {
			value = result;
			pos += i + 1;
			return true;
		}
	}
	return false;
}

inline int64_t AvroZigZagDecode(uint64_t value) {
	return static_cast<int64_t>((value >> 1) ^ (~(value & 1) + 1));
}

inline uint64_t AvroZigZagEncode(int64_t value) {
	return (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
}

inline bool AvroReadLong(const char *&pos, int64_t &value) {
	uint64_t raw;
	if (!AvroReadVarint(pos, raw)) {
		return false;
	}
	value = AvroZigZagDecode(raw);
	return true;
}

inline idx_t AvroWriteLong(int64_t value, data_ptr_t out) {
	uint64_t raw = AvroZigZagEncode(value);
	idx_t size = 0;
	while (raw >= 0x80) {
		out[size++] = static_cast<uint8_t>(raw | 0x80);
		raw >>= 7;
	}
	out[size++] = static_cast<uint8_t>(raw);
	return size;
}

struct AvroMetadataEntry {
	string key;
	string value;
};

struct AvroHeader {
	vector<AvroMetadataEntry> metadata;
	string schema;
	AvroCodec codec = AvroCodec::NONE;
	char sync[AVRO_SYNC_SIZE] = {};
	idx_t size = 0;
};

bool AvroParseHeader(const char *data, idx_t size, AvroHeader &header);
void AvroWriteHeader(const vector<AvroMetadataEntry> &metadata, const char *sync, string &out);

class AvroDecompressor {
public:
	explicit AvroDecompressor(Allocator &allocator);
	~AvroDecompressor();
	AvroDecompressor(const AvroDecompressor &) = delete;
	AvroDecompressor &operator=(const AvroDecompressor &) = delete;

	idx_t Decompress(AvroCodec codec, const char *data, idx_t size, AllocatedData &out);

private:
	data_ptr_t Reserve(AllocatedData &out, idx_t used, idx_t need);
	idx_t Inflate(const char *data, idx_t size, AllocatedData &out);
	idx_t Snappy(const char *data, idx_t size, AllocatedData &out);
	idx_t Zstandard(const char *data, idx_t size, AllocatedData &out);

private:
	Allocator &allocator;
	z_stream inflate_stream;
	bool inflate_ready = false;
	ZSTD_DCtx *zstd = nullptr;
};

class AvroCompressor {
public:
	AvroCompressor(Allocator &allocator, AvroCodec codec);
	~AvroCompressor();
	AvroCompressor(const AvroCompressor &) = delete;
	AvroCompressor &operator=(const AvroCompressor &) = delete;

	std::string_view Compress(const char *data, idx_t size);

private:
	data_ptr_t Reserve(idx_t need);

private:
	Allocator &allocator;
	const AvroCodec codec;
	z_stream deflate_stream;
	bool deflate_ready = false;
	ZSTD_CCtx *zstd = nullptr;
	AllocatedData out;
};

} // namespace duckdb
