#include "avro_binary.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"

#define ZLIB_CONST
#include <zlib.h>

namespace duckdb {

namespace {

class HeaderReader {
public:
	HeaderReader(const char *data, idx_t size) : pos(data), end(data + size) {
	}

	bool Long(int64_t &value) {
		const auto available = static_cast<idx_t>(end - pos);
		uint64_t result = 0;
		for (idx_t i = 0; i < AVRO_MAX_VARINT_SIZE; i++) {
			if (i >= available) {
				return false;
			}
			const auto byte = static_cast<uint8_t>(pos[i]);
			result |= static_cast<uint64_t>(byte & 0x7F) << (7 * i);
			if (byte < 0x80) {
				pos += i + 1;
				value = AvroZigZagDecode(result);
				return true;
			}
		}
		throw InvalidInputException("Avro file header has an invalid number");
	}

	bool Bytes(string &out) {
		int64_t length;
		if (!Long(length)) {
			return false;
		}
		if (length < 0) {
			throw InvalidInputException("Avro file header has a negative length");
		}
		if (static_cast<idx_t>(end - pos) < static_cast<idx_t>(length)) {
			return false;
		}
		out.assign(pos, static_cast<idx_t>(length));
		pos += length;
		return true;
	}

public:
	const char *pos;
	const char *end;
};

void AppendLong(string &out, int64_t value) {
	uint8_t bytes[AVRO_MAX_VARINT_SIZE];
	const idx_t size = AvroWriteLong(value, bytes);
	out.append(const_char_ptr_cast(bytes), size);
}

void AppendBytes(string &out, const string &value) {
	AppendLong(out, NumericCast<int64_t>(value.size()));
	out += value;
}

uint32_t LoadBigEndian(const char *data) {
	auto bytes = reinterpret_cast<const uint8_t *>(data);
	return (static_cast<uint32_t>(bytes[0]) << 24) | (static_cast<uint32_t>(bytes[1]) << 16) |
	       (static_cast<uint32_t>(bytes[2]) << 8) | static_cast<uint32_t>(bytes[3]);
}

void StoreBigEndian(uint32_t value, data_ptr_t out) {
	out[0] = static_cast<uint8_t>(value >> 24);
	out[1] = static_cast<uint8_t>(value >> 16);
	out[2] = static_cast<uint8_t>(value >> 8);
	out[3] = static_cast<uint8_t>(value);
}

[[noreturn]] void ThrowCorrupt(AvroCodec codec, const string &detail) {
	throw InvalidInputException("Avro %s block is corrupt: %s", string(AvroCodecName(codec)), detail);
}

BlockCodec AvroBlockCodec(AvroCodec codec) {
	switch (codec) {
	case AvroCodec::DEFLATE:
		return BlockCodec::DEFLATE;
	case AvroCodec::SNAPPY:
		return BlockCodec::SNAPPY;
	case AvroCodec::ZSTANDARD:
		return BlockCodec::ZSTD;
	default:
		throw InternalException("An Avro block without a codec reached the block codecs");
	}
}

} // namespace

AvroCodec AvroCodecFromName(std::string_view name) {
	if (name == "null") {
		return AvroCodec::NONE;
	}
	if (name == "deflate") {
		return AvroCodec::DEFLATE;
	}
	if (name == "snappy") {
		return AvroCodec::SNAPPY;
	}
	if (name == "zstandard") {
		return AvroCodec::ZSTANDARD;
	}
	throw InvalidInputException("Unknown codec %s, the supported codecs are null, deflate, snappy and zstandard",
	                            string(name));
}

std::string_view AvroCodecName(AvroCodec codec) {
	switch (codec) {
	case AvroCodec::NONE:
		return "null";
	case AvroCodec::DEFLATE:
		return "deflate";
	case AvroCodec::SNAPPY:
		return "snappy";
	case AvroCodec::ZSTANDARD:
		return "zstandard";
	}
	throw InternalException("Unknown Avro codec");
}

bool AvroParseHeader(const char *data, idx_t size, AvroHeader &header) {
	if (memcmp(data, AVRO_MAGIC, MinValue(size, AVRO_MAGIC_SIZE)) != 0) {
		throw InvalidInputException("Incorrect Avro container file magic number");
	}
	if (size < AVRO_MAGIC_SIZE) {
		return false;
	}
	HeaderReader reader(data + AVRO_MAGIC_SIZE, size - AVRO_MAGIC_SIZE);
	header.metadata.clear();
	while (true) {
		int64_t count;
		if (!reader.Long(count)) {
			return false;
		}
		if (count == 0) {
			break;
		}
		if (count < 0) {
			if (count == NumericLimits<int64_t>::Minimum()) {
				throw InvalidInputException("Avro file header has an invalid metadata count");
			}
			count = -count;
			int64_t bytes;
			if (!reader.Long(bytes)) {
				return false;
			}
		}
		for (int64_t i = 0; i < count; i++) {
			AvroMetadataEntry entry;
			if (!reader.Bytes(entry.key) || !reader.Bytes(entry.value)) {
				return false;
			}
			header.metadata.push_back(std::move(entry));
		}
	}
	if (static_cast<idx_t>(reader.end - reader.pos) < AVRO_SYNC_SIZE) {
		return false;
	}
	memcpy(header.sync, reader.pos, AVRO_SYNC_SIZE);
	header.size = static_cast<idx_t>(reader.pos - data) + AVRO_SYNC_SIZE;
	bool has_schema = false;
	header.codec = AvroCodec::NONE;
	for (auto &entry : header.metadata) {
		if (entry.key == "avro.schema") {
			header.schema = entry.value;
			has_schema = true;
		} else if (entry.key == "avro.codec") {
			header.codec = AvroCodecFromName(entry.value);
		}
	}
	if (!has_schema) {
		throw InvalidInputException("Avro file header has no \"avro.schema\"");
	}
	return true;
}

void AvroWriteHeader(const vector<AvroMetadataEntry> &metadata, const char *sync, string &out) {
	out.append(AVRO_MAGIC, AVRO_MAGIC_SIZE);
	if (!metadata.empty()) {
		AppendLong(out, NumericCast<int64_t>(metadata.size()));
		for (auto &entry : metadata) {
			AppendBytes(out, entry.key);
			AppendBytes(out, entry.value);
		}
	}
	AppendLong(out, 0);
	out.append(sync, AVRO_SYNC_SIZE);
}

AvroDecompressor::AvroDecompressor(Allocator &allocator_p) : allocator(allocator_p) {
}

idx_t AvroDecompressor::Decompress(AvroCodec codec, const char *data, idx_t size, AllocatedData &out) {
	auto compressed = size;
	if (codec == AvroCodec::SNAPPY) {
		if (size < sizeof(uint32_t)) {
			ThrowCorrupt(codec, "the block is shorter than its checksum");
		}
		compressed -= sizeof(uint32_t);
	}
	idx_t length;
	string error;
	if (!decompressor.TryDecompress(AvroBlockCodec(codec), const_data_ptr_cast(data), compressed, allocator,
	                                AVRO_PADDING, out, length, error)) {
		ThrowCorrupt(codec, error);
	}
	if (codec == AvroCodec::SNAPPY && crc32_z(0, out.get(), length) != LoadBigEndian(data + compressed)) {
		ThrowCorrupt(codec, "CRC32 checksum mismatch");
	}
	return length;
}

AvroCompressor::AvroCompressor(Allocator &allocator_p, AvroCodec codec_p) : allocator(allocator_p), codec(codec_p) {
	if (codec != AvroCodec::NONE) {
		compressor = make_uniq<BlockCompressor>(AvroBlockCodec(codec));
	}
}

data_ptr_t AvroCompressor::Reserve(idx_t need) {
	if (out.GetSize() < need) {
		out = allocator.Allocate(MaxValue(need, out.GetSize() * 2));
	}
	return out.get();
}

std::string_view AvroCompressor::Compress(const char *data, idx_t size) {
	if (!compressor) {
		return {data, size};
	}
	const auto checksum = codec == AvroCodec::SNAPPY ? sizeof(uint32_t) : 0;
	const auto bound = compressor->MaxCompressedSize(size);
	auto base = Reserve(bound + checksum);
	auto length = compressor->Compress(const_data_ptr_cast(data), size, base, bound);
	if (checksum) {
		StoreBigEndian(static_cast<uint32_t>(crc32_z(0, const_data_ptr_cast(data), size)), base + length);
		length += checksum;
	}
	return {const_char_ptr_cast(base), length};
}

} // namespace duckdb
