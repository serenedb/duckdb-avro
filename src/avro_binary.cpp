#include "avro_binary.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "snappy.h"

namespace duckdb {

namespace {

constexpr idx_t MIN_DECOMPRESS_BYTES = 64 * 1024;

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

uInt StreamBytes(idx_t size) {
	if (size > NumericLimits<uInt>::Maximum()) {
		throw InvalidInputException("Avro block of %llu bytes is too large to compress or decompress", size);
	}
	return static_cast<uInt>(size);
}

[[noreturn]] void ThrowCorrupt(AvroCodec codec, const char *detail) {
	throw InvalidInputException("Avro %s block is corrupt: %s", string(AvroCodecName(codec)), detail);
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

AvroDecompressor::~AvroDecompressor() {
	if (inflate_ready) {
		inflateEnd(&inflate_stream);
	}
	if (zstd) {
		ZSTD_freeDCtx(zstd);
	}
}

data_ptr_t AvroDecompressor::Reserve(AllocatedData &out, idx_t used, idx_t need) {
	const idx_t want = used + need + AVRO_PADDING;
	if (out.GetSize() < want) {
		auto grown = allocator.Allocate(MaxValue(want, out.GetSize() * 2));
		if (used) {
			memcpy(grown.get(), out.get(), used);
		}
		out = std::move(grown);
	}
	return out.get();
}

idx_t AvroDecompressor::Decompress(AvroCodec codec, const char *data, idx_t size, AllocatedData &out) {
	idx_t result;
	switch (codec) {
	case AvroCodec::DEFLATE:
		result = Inflate(data, size, out);
		break;
	case AvroCodec::SNAPPY:
		result = Snappy(data, size, out);
		break;
	case AvroCodec::ZSTANDARD:
		result = Zstandard(data, size, out);
		break;
	default:
		throw InternalException("Avro block without a codec reached the decompressor");
	}
	memset(out.get() + result, 0, AVRO_PADDING);
	return result;
}

idx_t AvroDecompressor::Inflate(const char *data, idx_t size, AllocatedData &out) {
	if (!inflate_ready) {
		memset(&inflate_stream, 0, sizeof(inflate_stream));
		if (inflateInit2(&inflate_stream, -MAX_WBITS) != Z_OK) {
			throw InternalException("Failed to initialize the Avro deflate decoder");
		}
		inflate_ready = true;
	} else if (inflateReset(&inflate_stream) != Z_OK) {
		throw InternalException("Failed to reset the Avro deflate decoder");
	}
	inflate_stream.next_in = reinterpret_cast<const Bytef *>(data);
	inflate_stream.avail_in = StreamBytes(size);
	idx_t used = 0;
	idx_t chunk = MaxValue(size * 4, MIN_DECOMPRESS_BYTES);
	while (true) {
		const uInt room = StreamBytes(chunk);
		auto base = Reserve(out, used, room);
		inflate_stream.next_out = base + used;
		inflate_stream.avail_out = room;
		const int ret = inflate(&inflate_stream, Z_NO_FLUSH);
		used += room - inflate_stream.avail_out;
		if (ret == Z_STREAM_END) {
			return used;
		}
		if (ret != Z_OK && ret != Z_BUF_ERROR) {
			ThrowCorrupt(AvroCodec::DEFLATE, inflate_stream.msg ? inflate_stream.msg : zError(ret));
		}
		if (inflate_stream.avail_in == 0 && inflate_stream.avail_out != 0) {
			ThrowCorrupt(AvroCodec::DEFLATE, "the compressed data ends early");
		}
		chunk = MaxValue(chunk, used);
	}
}

idx_t AvroDecompressor::Snappy(const char *data, idx_t size, AllocatedData &out) {
	if (size < sizeof(uint32_t)) {
		ThrowCorrupt(AvroCodec::SNAPPY, "the block is shorter than its checksum");
	}
	const idx_t compressed = size - sizeof(uint32_t);
	size_t length;
	if (!duckdb_snappy::GetUncompressedLength(data, compressed, &length)) {
		ThrowCorrupt(AvroCodec::SNAPPY, "invalid uncompressed length");
	}
	auto base = Reserve(out, 0, length);
	if (!duckdb_snappy::RawUncompress(data, compressed, char_ptr_cast(base))) {
		ThrowCorrupt(AvroCodec::SNAPPY, "invalid compressed data");
	}
	if (crc32_z(0, base, length) != LoadBigEndian(data + compressed)) {
		ThrowCorrupt(AvroCodec::SNAPPY, "CRC32 checksum mismatch");
	}
	return length;
}

idx_t AvroDecompressor::Zstandard(const char *data, idx_t size, AllocatedData &out) {
	if (!zstd) {
		zstd = ZSTD_createDCtx();
		if (!zstd) {
			throw OutOfMemoryException("Failed to allocate the Avro zstandard decoder");
		}
	} else {
		ZSTD_DCtx_reset(zstd, ZSTD_reset_session_only);
	}
	const auto known = ZSTD_getFrameContentSize(data, size);
	idx_t chunk = known == ZSTD_CONTENTSIZE_UNKNOWN || known == ZSTD_CONTENTSIZE_ERROR
	                  ? MaxValue(size * 4, MIN_DECOMPRESS_BYTES)
	                  : MaxValue<idx_t>(known, 1);
	ZSTD_inBuffer input {data, size, 0};
	idx_t used = 0;
	while (true) {
		auto base = Reserve(out, used, chunk);
		ZSTD_outBuffer output {base, used + chunk, used};
		const size_t ret = ZSTD_decompressStream(zstd, &output, &input);
		if (ZSTD_isError(ret)) {
			ThrowCorrupt(AvroCodec::ZSTANDARD, ZSTD_getErrorName(ret));
		}
		used = output.pos;
		if (input.pos == input.size) {
			if (ret == 0) {
				return used;
			}
			if (output.pos < output.size) {
				ThrowCorrupt(AvroCodec::ZSTANDARD, "the compressed data ends early");
			}
		}
		chunk = MaxValue(chunk, used);
	}
}

AvroCompressor::AvroCompressor(Allocator &allocator_p, AvroCodec codec_p) : allocator(allocator_p), codec(codec_p) {
}

AvroCompressor::~AvroCompressor() {
	if (deflate_ready) {
		deflateEnd(&deflate_stream);
	}
	if (zstd) {
		ZSTD_freeCCtx(zstd);
	}
}

data_ptr_t AvroCompressor::Reserve(idx_t need) {
	if (out.GetSize() < need) {
		out = allocator.Allocate(MaxValue(need, out.GetSize() * 2));
	}
	return out.get();
}

std::string_view AvroCompressor::Compress(const char *data, idx_t size) {
	switch (codec) {
	case AvroCodec::NONE:
		return {data, size};
	case AvroCodec::DEFLATE: {
		if (!deflate_ready) {
			memset(&deflate_stream, 0, sizeof(deflate_stream));
			if (deflateInit2(&deflate_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -MAX_WBITS, 8, Z_DEFAULT_STRATEGY) !=
			    Z_OK) {
				throw InternalException("Failed to initialize the Avro deflate encoder");
			}
			deflate_ready = true;
		} else if (deflateReset(&deflate_stream) != Z_OK) {
			throw InternalException("Failed to reset the Avro deflate encoder");
		}
		const idx_t bound = deflateBound(&deflate_stream, StreamBytes(size));
		auto base = Reserve(bound);
		deflate_stream.next_in = reinterpret_cast<const Bytef *>(data);
		deflate_stream.avail_in = StreamBytes(size);
		deflate_stream.next_out = base;
		deflate_stream.avail_out = StreamBytes(bound);
		if (deflate(&deflate_stream, Z_FINISH) != Z_STREAM_END) {
			throw InternalException("Avro deflate encoder did not finish its block");
		}
		return {const_char_ptr_cast(base), bound - deflate_stream.avail_out};
	}
	case AvroCodec::SNAPPY: {
		const idx_t bound = duckdb_snappy::MaxCompressedLength(size);
		auto base = Reserve(bound + sizeof(uint32_t));
		size_t length;
		duckdb_snappy::RawCompress(data, size, char_ptr_cast(base), &length);
		StoreBigEndian(static_cast<uint32_t>(crc32_z(0, reinterpret_cast<const Bytef *>(data), size)), base + length);
		return {const_char_ptr_cast(base), length + sizeof(uint32_t)};
	}
	case AvroCodec::ZSTANDARD: {
		if (!zstd) {
			zstd = ZSTD_createCCtx();
			if (!zstd) {
				throw OutOfMemoryException("Failed to allocate the Avro zstandard encoder");
			}
		}
		const idx_t bound = ZSTD_compressBound(size);
		auto base = Reserve(bound);
		const size_t length = ZSTD_compressCCtx(zstd, base, bound, data, size, ZSTD_CLEVEL_DEFAULT);
		if (ZSTD_isError(length)) {
			throw InternalException("Avro zstandard encoder failed: %s", ZSTD_getErrorName(length));
		}
		return {const_char_ptr_cast(base), length};
	}
	}
	throw InternalException("Unknown Avro codec");
}

} // namespace duckdb
