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
#include "duckdb/common/serializer/encoding_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/formats/block_codecs.hpp"

#include <string_view>

namespace duckdb {

static constexpr idx_t AVRO_SYNC_SIZE = 16;
static constexpr idx_t AVRO_MAGIC_SIZE = 4;
static constexpr char AVRO_MAGIC[AVRO_MAGIC_SIZE] = {'O', 'b', 'j', 1};
static constexpr idx_t AVRO_MAX_VARINT_SIZE = EncodingUtil::MaxLEB128Size<uint64_t>();
static constexpr idx_t AVRO_PADDING = 64;

enum class AvroCodec : uint8_t { NONE, DEFLATE, SNAPPY, ZSTANDARD };

AvroCodec AvroCodecFromName(std::string_view name);
std::string_view AvroCodecName(AvroCodec codec);

inline bool AvroReadLong(const char *&pos, int64_t &value) {
	uint64_t raw;
	const auto size = EncodingUtil::TryDecodeUnsignedLEB128(const_data_ptr_cast(pos), AVRO_MAX_VARINT_SIZE, raw);
	if (size == 0) {
		return false;
	}
	pos += size;
	value = EncodingUtil::ZigZagDecode(raw);
	return true;
}

inline bool AvroReadBlockHeader(const char *&pos, const char *limit, int64_t &count, int64_t &size) {
	return AvroReadLong(pos, count) && pos <= limit && AvroReadLong(pos, size) && pos <= limit;
}

inline idx_t AvroWriteLong(int64_t value, data_ptr_t out) {
	return EncodingUtil::EncodeUnsignedLEB128(out, EncodingUtil::ZigZagEncode(value));
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

	idx_t Decompress(AvroCodec codec, const char *data, idx_t size, AllocatedData &out);

private:
	Allocator &allocator;
	BlockDecompressor decompressor;
};

class AvroCompressor {
public:
	AvroCompressor(Allocator &allocator, AvroCodec codec);

	std::string_view Compress(const char *data, idx_t size);

private:
	data_ptr_t Reserve(idx_t need);

private:
	Allocator &allocator;
	const AvroCodec codec;
	unique_ptr<BlockCompressor> compressor;
	AllocatedData out;
};

} // namespace duckdb
