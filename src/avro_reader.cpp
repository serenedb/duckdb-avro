#include "avro_reader.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/union_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_handle.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "simdutf.h"

namespace duckdb {

namespace {

constexpr idx_t HEADER_READ_BYTES = 64 * 1024;
constexpr idx_t LOOKUP_BYTES = 64 * 1024;
constexpr idx_t READ_BYTES = 1024 * 1024;
constexpr idx_t MIN_RANGE_BYTES = 1024 * 1024;
constexpr idx_t MAX_RANGE_BYTES = 8 * 1024 * 1024;
constexpr idx_t RECORD_INDEX_BITS = 24;
constexpr idx_t MAX_BLOCK_OFFSET = idx_t(1) << (63 - RECORD_INDEX_BITS);
constexpr int64_t MAX_EMPTY_ITEMS = int64_t(1) << 32;

[[noreturn]] void ThrowData(const char *detail) {
	throw InvalidInputException(detail);
}

[[gnu::always_inline]] inline int64_t ReadLong(const char *&pos, const char *end) {
	int64_t value;
	if (!AvroReadLong(pos, value) || pos > end) {
		ThrowData("a number is malformed or runs past the end of its block");
	}
	return value;
}

[[gnu::always_inline]] inline void SkipLong(const char *&pos, const char *end) {
	auto bytes = reinterpret_cast<const uint8_t *>(pos);
	for (idx_t i = 0; i < AVRO_MAX_VARINT_SIZE; i++) {
		if (bytes[i] < 0x80) {
			pos += i + 1;
			if (pos > end) {
				ThrowData("a number runs past the end of its block");
			}
			return;
		}
	}
	ThrowData("a number is malformed");
}

inline int32_t ReadInt(const char *&pos, const char *end) {
	const int64_t value = ReadLong(pos, end);
	if (value < NumericLimits<int32_t>::Minimum() || value > NumericLimits<int32_t>::Maximum()) {
		ThrowData("an int is out of range");
	}
	return static_cast<int32_t>(value);
}

inline idx_t ReadLength(const char *&pos, const char *end) {
	const int64_t length = ReadLong(pos, end);
	if (length < 0 || static_cast<idx_t>(length) > static_cast<idx_t>(end - pos)) {
		ThrowData("a length is negative or runs past the end of its block");
	}
	return static_cast<idx_t>(length);
}

inline void Need(const char *pos, const char *end, idx_t bytes) {
	if (bytes > static_cast<idx_t>(end - pos)) {
		ThrowData("a value runs past the end of its block");
	}
}

inline idx_t ReadBranch(const AvroNode &node, const char *&pos, const char *end) {
	const int64_t branch = ReadLong(pos, end);
	if (branch < 0 || static_cast<idx_t>(branch) >= node.children.size()) {
		ThrowData("a union branch index is out of range");
	}
	return static_cast<idx_t>(branch);
}

[[gnu::always_inline]] inline int64_t TimeMicros(int64_t micros) {
	if (micros < 0 || micros > Interval::MICROS_PER_DAY) {
		ThrowData("a time value is outside of a day");
	}
	return micros;
}

inline string_t StringRef(const char *pos, idx_t length) {
	if (length > NumericLimits<uint32_t>::Maximum()) {
		ThrowData("a string or bytes value is larger than 4 GiB");
	}
	return string_t(pos, static_cast<uint32_t>(length));
}

[[gnu::always_inline]] inline bool IsAscii(const char *data, idx_t size) {
	uint64_t bits = 0;
	idx_t i = 0;
	for (; i + sizeof(uint64_t) <= size; i += sizeof(uint64_t)) {
		uint64_t word;
		memcpy(&word, data + i, sizeof(word));
		bits |= word;
	}
	for (; i < size; i++) {
		bits |= static_cast<uint8_t>(data[i]);
	}
	return (bits & 0x8080808080808080ULL) == 0;
}

[[gnu::always_inline]] inline void CheckUtf8(const char *pos, idx_t length) {
	if (!IsAscii(pos, length) && !simdutf::validate_utf8(pos, length)) {
		ThrowData("a string is not valid UTF-8");
	}
}

bool CanBeEmpty(const AvroNode &node) {
	switch (node.kind) {
	case AvroKind::NULL_TYPE:
		return true;
	case AvroKind::FIXED:
		return node.size == 0;
	case AvroKind::RECORD:
		for (auto &child : node.children) {
			if (!CanBeEmpty(child)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

idx_t ReadBlockCount(const AvroNode &item, const char *&pos, const char *end, idx_t &bytes) {
	int64_t count = ReadLong(pos, end);
	bytes = DConstants::INVALID_INDEX;
	if (count < 0) {
		if (count == NumericLimits<int64_t>::Minimum()) {
			ThrowData("an array or map block count is out of range");
		}
		count = -count;
		bytes = ReadLength(pos, end);
	}
	const bool empty_items = CanBeEmpty(item);
	if ((!empty_items && count > end - pos) || (empty_items && count > MAX_EMPTY_ITEMS)) {
		ThrowData("an array or map block count exceeds its data");
	}
	return static_cast<idx_t>(count);
}

void Skip(const AvroNode &node, const char *&pos, const char *end);

void SkipBlocks(const AvroNode &node, const char *&pos, const char *end) {
	while (true) {
		idx_t bytes;
		const idx_t count = ReadBlockCount(node.children[0], pos, end, bytes);
		if (count == 0) {
			return;
		}
		if (bytes != DConstants::INVALID_INDEX) {
			pos += bytes;
			continue;
		}
		for (idx_t i = 0; i < count; i++) {
			if (node.kind == AvroKind::MAP) {
				pos += ReadLength(pos, end);
			}
			Skip(node.children[0], pos, end);
		}
	}
}

void Skip(const AvroNode &node, const char *&pos, const char *end) {
	switch (node.kind) {
	case AvroKind::NULL_TYPE:
		return;
	case AvroKind::BOOLEAN:
		Need(pos, end, 1);
		pos++;
		return;
	case AvroKind::INT:
	case AvroKind::LONG:
	case AvroKind::ENUM:
		ReadLong(pos, end);
		return;
	case AvroKind::FLOAT:
		Need(pos, end, sizeof(float));
		pos += sizeof(float);
		return;
	case AvroKind::DOUBLE:
		Need(pos, end, sizeof(double));
		pos += sizeof(double);
		return;
	case AvroKind::BYTES:
	case AvroKind::STRING:
		pos += ReadLength(pos, end);
		return;
	case AvroKind::FIXED:
		Need(pos, end, node.size);
		pos += node.size;
		return;
	case AvroKind::RECORD:
		for (auto &child : node.children) {
			Skip(child, pos, end);
		}
		return;
	case AvroKind::ARRAY:
	case AvroKind::MAP:
		SkipBlocks(node, pos, end);
		return;
	case AvroKind::UNION:
		Skip(node.children[ReadBranch(node, pos, end)], pos, end);
		return;
	}
}

void SetNull(Vector &target, idx_t row) {
	FlatVector::SetNull(target, row, true);
	if (target.GetType().InternalType() == PhysicalType::LIST) {
		FlatVector::GetDataMutableUnsafe<list_entry_t>(target)[row] = list_entry_t(ListVector::GetListSize(target), 0);
	}
}

template <class T>
void StoreDecimal(int64_t value, Vector &target, idx_t row) {
	if (value < NumericLimits<T>::Minimum() || value > NumericLimits<T>::Maximum()) {
		ThrowData("a decimal value does not fit its precision");
	}
	FlatVector::GetDataMutableUnsafe<T>(target)[row] = static_cast<T>(value);
}

void DecodeDecimal(const AvroNode &node, const char *data, idx_t size, Vector &target, idx_t row) {
	auto bytes = reinterpret_cast<const uint8_t *>(data);
	const bool negative = size != 0 && (bytes[0] & 0x80) != 0;
	if (size <= sizeof(int64_t)) {
		uint64_t value = negative ? NumericLimits<uint64_t>::Maximum() : 0;
		for (idx_t i = 0; i < size; i++) {
			value = (value << 8) | bytes[i];
		}
		const auto signed_value = static_cast<int64_t>(value);
		switch (node.physical) {
		case PhysicalType::INT16:
			StoreDecimal<int16_t>(signed_value, target, row);
			return;
		case PhysicalType::INT32:
			StoreDecimal<int32_t>(signed_value, target, row);
			return;
		case PhysicalType::INT64:
			FlatVector::GetDataMutableUnsafe<int64_t>(target)[row] = signed_value;
			return;
		default:
			FlatVector::GetDataMutableUnsafe<hugeint_t>(target)[row] = hugeint_t(signed_value);
			return;
		}
	}
	const uint8_t sign = negative ? 0xFF : 0x00;
	idx_t skip = size > sizeof(hugeint_t) ? size - sizeof(hugeint_t) : 0;
	for (idx_t i = 0; i < skip; i++) {
		if (bytes[i] != sign) {
			ThrowData("a decimal value does not fit its precision");
		}
	}
	if (skip && ((bytes[skip] & 0x80) != 0) != negative) {
		ThrowData("a decimal value does not fit its precision");
	}
	uint64_t upper = negative ? NumericLimits<uint64_t>::Maximum() : 0;
	uint64_t lower = upper;
	for (idx_t i = skip; i < size; i++) {
		upper = (upper << 8) | (lower >> 56);
		lower = (lower << 8) | bytes[i];
	}
	hugeint_t value;
	value.upper = static_cast<int64_t>(upper);
	value.lower = lower;
	if (node.physical == PhysicalType::INT128) {
		FlatVector::GetDataMutableUnsafe<hugeint_t>(target)[row] = value;
		return;
	}
	int64_t narrow;
	if (!Hugeint::TryCast<int64_t>(value, narrow)) {
		ThrowData("a decimal value does not fit its precision");
	}
	switch (node.physical) {
	case PhysicalType::INT16:
		StoreDecimal<int16_t>(narrow, target, row);
		return;
	case PhysicalType::INT32:
		StoreDecimal<int32_t>(narrow, target, row);
		return;
	default:
		FlatVector::GetDataMutableUnsafe<int64_t>(target)[row] = narrow;
		return;
	}
}

uint32_t LoadLittleEndian(const char *data) {
	auto bytes = reinterpret_cast<const uint8_t *>(data);
	return static_cast<uint32_t>(bytes[0]) | (static_cast<uint32_t>(bytes[1]) << 8) |
	       (static_cast<uint32_t>(bytes[2]) << 16) | (static_cast<uint32_t>(bytes[3]) << 24);
}

void Decode(const AvroNode &node, const char *&pos, const char *end, Vector &target, idx_t row);

void DecodeArray(const AvroNode &node, const char *&pos, const char *end, Vector &target, idx_t row) {
	const idx_t offset = ListVector::GetListSize(target);
	idx_t total = 0;
	while (true) {
		idx_t bytes;
		const idx_t count = ReadBlockCount(node.children[0], pos, end, bytes);
		if (count == 0) {
			break;
		}
		ListVector::Reserve(target, offset + total + count);
		auto &child = ListVector::GetChildMutable(target);
		for (idx_t i = 0; i < count; i++) {
			Decode(node.children[0], pos, end, child, offset + total + i);
		}
		total += count;
	}
	FlatVector::GetDataMutableUnsafe<list_entry_t>(target)[row] = list_entry_t(offset, total);
	ListVector::SetListSize(target, offset + total);
}

void CheckMapKeys(const Vector &target, idx_t row) {
	auto &validity = FlatVector::Validity(MapVector::GetKeys(target));
	if (validity.AllValid()) {
		return;
	}
	const auto entry = FlatVector::GetData<list_entry_t>(target)[row];
	for (idx_t i = entry.offset; i < entry.offset + entry.length; i++) {
		if (!validity.RowIsValid(i)) {
			ThrowData("a map key is NULL");
		}
	}
}

void DecodeMap(const AvroNode &node, const char *&pos, const char *end, Vector &target, idx_t row) {
	const idx_t offset = ListVector::GetListSize(target);
	idx_t total = 0;
	while (true) {
		idx_t bytes;
		const idx_t count = ReadBlockCount(node.children[0], pos, end, bytes);
		if (count == 0) {
			break;
		}
		ListVector::Reserve(target, offset + total + count);
		auto &keys = MapVector::GetKeys(target);
		auto &values = MapVector::GetValues(target);
		auto key_data = FlatVector::GetDataMutableUnsafe<string_t>(keys);
		for (idx_t i = 0; i < count; i++) {
			const idx_t length = ReadLength(pos, end);
			CheckUtf8(pos, length);
			key_data[offset + total + i] = StringRef(pos, length);
			pos += length;
			Decode(node.children[0], pos, end, values, offset + total + i);
		}
		total += count;
	}
	FlatVector::GetDataMutableUnsafe<list_entry_t>(target)[row] = list_entry_t(offset, total);
	ListVector::SetListSize(target, offset + total);
}

void DecodeUnion(const AvroNode &node, const char *&pos, const char *end, Vector &target, idx_t row) {
	const idx_t branch = ReadBranch(node, pos, end);
	const idx_t member = node.members[branch];
	if (member == DConstants::INVALID_INDEX) {
		Skip(node.children[branch], pos, end);
		SetNull(target, row);
		return;
	}
	if (node.decode == AvroDecode::NULLABLE) {
		Decode(node.children[branch], pos, end, target, row);
		return;
	}
	FlatVector::GetDataMutableUnsafe<union_tag_t>(UnionVector::GetTags(target))[row] = static_cast<union_tag_t>(member);
	const idx_t member_count = UnionType::GetMemberCount(target.GetType());
	for (idx_t i = 0; i < member_count; i++) {
		if (i != member) {
			SetNull(UnionVector::GetMember(target, i), row);
		}
	}
	Decode(node.children[branch], pos, end, UnionVector::GetMember(target, member), row);
}

void Decode(const AvroNode &node, const char *&pos, const char *end, Vector &target, idx_t row) {
	switch (node.decode) {
	case AvroDecode::NULL_VALUE:
	case AvroDecode::EMPTY_RECORD:
		SetNull(target, row);
		return;
	case AvroDecode::BOOLEAN:
		Need(pos, end, 1);
		FlatVector::GetDataMutableUnsafe<bool>(target)[row] = *pos != 0;
		pos++;
		return;
	case AvroDecode::INT:
	case AvroDecode::DATE:
		FlatVector::GetDataMutableUnsafe<int32_t>(target)[row] = ReadInt(pos, end);
		return;
	case AvroDecode::TIME_MILLIS:
		FlatVector::GetDataMutableUnsafe<int64_t>(target)[row] =
		    TimeMicros(static_cast<int64_t>(ReadInt(pos, end)) * Interval::MICROS_PER_MSEC);
		return;
	case AvroDecode::TIME_MICROS:
		FlatVector::GetDataMutableUnsafe<int64_t>(target)[row] = TimeMicros(ReadLong(pos, end));
		return;
	case AvroDecode::LONG:
		FlatVector::GetDataMutableUnsafe<int64_t>(target)[row] = ReadLong(pos, end);
		return;
	case AvroDecode::TIMESTAMP_MILLIS: {
		int64_t micros;
		if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(ReadLong(pos, end), Interval::MICROS_PER_MSEC,
		                                                               micros)) {
			ThrowData("a timestamp-millis value is out of range");
		}
		FlatVector::GetDataMutableUnsafe<int64_t>(target)[row] = micros;
		return;
	}
	case AvroDecode::FLOAT:
		Need(pos, end, sizeof(float));
		memcpy(FlatVector::GetDataMutableUnsafe<float>(target) + row, pos, sizeof(float));
		pos += sizeof(float);
		return;
	case AvroDecode::DOUBLE:
		Need(pos, end, sizeof(double));
		memcpy(FlatVector::GetDataMutableUnsafe<double>(target) + row, pos, sizeof(double));
		pos += sizeof(double);
		return;
	case AvroDecode::STRING: {
		const idx_t length = ReadLength(pos, end);
		CheckUtf8(pos, length);
		FlatVector::GetDataMutableUnsafe<string_t>(target)[row] = StringRef(pos, length);
		pos += length;
		return;
	}
	case AvroDecode::UUID_STRING: {
		const idx_t length = ReadLength(pos, end);
		hugeint_t value;
		if (!BaseUUID::FromString(std::string_view(pos, length), value, true)) {
			ThrowData("a uuid string is not a valid UUID");
		}
		FlatVector::GetDataMutableUnsafe<hugeint_t>(target)[row] = value;
		pos += length;
		return;
	}
	case AvroDecode::BYTES: {
		const idx_t length = ReadLength(pos, end);
		FlatVector::GetDataMutableUnsafe<string_t>(target)[row] = StringRef(pos, length);
		pos += length;
		return;
	}
	case AvroDecode::DECIMAL_BYTES: {
		const idx_t length = ReadLength(pos, end);
		DecodeDecimal(node, pos, length, target, row);
		pos += length;
		return;
	}
	case AvroDecode::FIXED:
		Need(pos, end, node.size);
		FlatVector::GetDataMutableUnsafe<string_t>(target)[row] = StringRef(pos, node.size);
		pos += node.size;
		return;
	case AvroDecode::UUID_FIXED:
		Need(pos, end, node.size);
		FlatVector::GetDataMutableUnsafe<hugeint_t>(target)[row] = BaseUUID::FromBlob(const_data_ptr_cast(pos));
		pos += node.size;
		return;
	case AvroDecode::DECIMAL_FIXED:
		Need(pos, end, node.size);
		DecodeDecimal(node, pos, node.size, target, row);
		pos += node.size;
		return;
	case AvroDecode::DURATION: {
		Need(pos, end, node.size);
		const uint32_t months = LoadLittleEndian(pos);
		const uint32_t days = LoadLittleEndian(pos + 4);
		const uint32_t millis = LoadLittleEndian(pos + 8);
		if (months > static_cast<uint32_t>(NumericLimits<int32_t>::Maximum()) ||
		    days > static_cast<uint32_t>(NumericLimits<int32_t>::Maximum())) {
			ThrowData("a duration is out of range");
		}
		interval_t value;
		value.months = static_cast<int32_t>(months);
		value.days = static_cast<int32_t>(days);
		value.micros = static_cast<int64_t>(millis) * Interval::MICROS_PER_MSEC;
		FlatVector::GetDataMutableUnsafe<interval_t>(target)[row] = value;
		pos += node.size;
		return;
	}
	case AvroDecode::ENUM: {
		const int64_t index = ReadLong(pos, end);
		if (index < 0 || static_cast<idx_t>(index) >= node.size) {
			ThrowData("an enum index is out of range");
		}
		switch (node.physical) {
		case PhysicalType::UINT8:
			FlatVector::GetDataMutableUnsafe<uint8_t>(target)[row] = static_cast<uint8_t>(index);
			return;
		case PhysicalType::UINT16:
			FlatVector::GetDataMutableUnsafe<uint16_t>(target)[row] = static_cast<uint16_t>(index);
			return;
		default:
			FlatVector::GetDataMutableUnsafe<uint32_t>(target)[row] = static_cast<uint32_t>(index);
			return;
		}
	}
	case AvroDecode::RECORD: {
		auto &entries = StructVector::GetEntries(target);
		for (idx_t i = 0; i < node.children.size(); i++) {
			Decode(node.children[i], pos, end, entries[i], row);
		}
		return;
	}
	case AvroDecode::ARRAY:
		DecodeArray(node, pos, end, target, row);
		return;
	case AvroDecode::ARRAY_MAP:
		DecodeArray(node, pos, end, target, row);
		CheckMapKeys(target, row);
		return;
	case AvroDecode::MAP:
		DecodeMap(node, pos, end, target, row);
		return;
	case AvroDecode::UNION:
	case AvroDecode::NULLABLE:
		DecodeUnion(node, pos, end, target, row);
		return;
	}
}

template <class T>
[[gnu::always_inline]] inline void Store(const AvroTarget &target, idx_t row, T value) {
	reinterpret_cast<T *>(target.data)[row] = value;
}

[[gnu::always_inline]] inline bool FastBranch(const AvroNode &node, const char *&pos, const char *end,
                                              const AvroTarget &target, idx_t row) {
	if (node.decode != AvroDecode::NULLABLE) {
		return true;
	}
	Need(pos, end, 1);
	const auto branch = static_cast<uint8_t>(*pos);
	if (branch == node.null_byte) {
		pos++;
		target.validity->SetInvalid(row);
		return false;
	}
	if (branch != node.value_byte) {
		Decode(node, pos, end, *target.vector, row);
		return false;
	}
	pos++;
	return true;
}

[[gnu::always_inline]] inline void DecodeFast(const AvroNode &node, const char *&pos, const char *end,
                                              const AvroTarget &target, idx_t row) {
	if (!FastBranch(node, pos, end, target, row)) {
		return;
	}
	switch (node.fast) {
	case AvroFast::BOOLEAN:
		Need(pos, end, 1);
		Store<bool>(target, row, *pos != 0);
		pos++;
		return;
	case AvroFast::INT:
		Store<int32_t>(target, row, ReadInt(pos, end));
		return;
	case AvroFast::TIME_MILLIS:
		Store<int64_t>(target, row, TimeMicros(static_cast<int64_t>(ReadInt(pos, end)) * Interval::MICROS_PER_MSEC));
		return;
	case AvroFast::TIME_MICROS:
		Store<int64_t>(target, row, TimeMicros(ReadLong(pos, end)));
		return;
	case AvroFast::LONG:
		Store<int64_t>(target, row, ReadLong(pos, end));
		return;
	case AvroFast::TIMESTAMP_MILLIS: {
		int64_t micros;
		if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(ReadLong(pos, end), Interval::MICROS_PER_MSEC,
		                                                               micros)) {
			ThrowData("a timestamp-millis value is out of range");
		}
		Store<int64_t>(target, row, micros);
		return;
	}
	case AvroFast::FLOAT:
		Need(pos, end, sizeof(float));
		memcpy(target.data + row * sizeof(float), pos, sizeof(float));
		pos += sizeof(float);
		return;
	case AvroFast::DOUBLE:
		Need(pos, end, sizeof(double));
		memcpy(target.data + row * sizeof(double), pos, sizeof(double));
		pos += sizeof(double);
		return;
	case AvroFast::STRING: {
		const idx_t length = ReadLength(pos, end);
		CheckUtf8(pos, length);
		Store<string_t>(target, row, StringRef(pos, length));
		pos += length;
		return;
	}
	case AvroFast::BYTES: {
		const idx_t length = ReadLength(pos, end);
		Store<string_t>(target, row, StringRef(pos, length));
		pos += length;
		return;
	}
	case AvroFast::NONE:
		break;
	}
	Decode(node, pos, end, *target.vector, row);
}

[[gnu::always_inline]] inline void SkipFast(const AvroNode &node, const char *&pos, const char *end) {
	if (node.decode == AvroDecode::NULLABLE) {
		Need(pos, end, 1);
		const auto branch = static_cast<uint8_t>(*pos);
		if (branch == node.null_byte) {
			pos++;
			return;
		}
		if (branch != node.value_byte) {
			Skip(node, pos, end);
			return;
		}
		pos++;
	}
	switch (node.fast) {
	case AvroFast::BOOLEAN:
		Need(pos, end, 1);
		pos++;
		return;
	case AvroFast::INT:
	case AvroFast::TIME_MILLIS:
	case AvroFast::TIME_MICROS:
	case AvroFast::LONG:
	case AvroFast::TIMESTAMP_MILLIS:
		SkipLong(pos, end);
		return;
	case AvroFast::FLOAT:
		Need(pos, end, sizeof(float));
		pos += sizeof(float);
		return;
	case AvroFast::DOUBLE:
		Need(pos, end, sizeof(double));
		pos += sizeof(double);
		return;
	case AvroFast::STRING:
	case AvroFast::BYTES:
		pos += ReadLength(pos, end);
		return;
	case AvroFast::NONE:
		break;
	}
	Skip(node, pos, end);
}

AvroFast FastOf(AvroDecode decode) {
	switch (decode) {
	case AvroDecode::BOOLEAN:
		return AvroFast::BOOLEAN;
	case AvroDecode::INT:
	case AvroDecode::DATE:
		return AvroFast::INT;
	case AvroDecode::TIME_MILLIS:
		return AvroFast::TIME_MILLIS;
	case AvroDecode::TIME_MICROS:
		return AvroFast::TIME_MICROS;
	case AvroDecode::LONG:
		return AvroFast::LONG;
	case AvroDecode::TIMESTAMP_MILLIS:
		return AvroFast::TIMESTAMP_MILLIS;
	case AvroDecode::FLOAT:
		return AvroFast::FLOAT;
	case AvroDecode::DOUBLE:
		return AvroFast::DOUBLE;
	case AvroDecode::STRING:
		return AvroFast::STRING;
	case AvroDecode::BYTES:
		return AvroFast::BYTES;
	default:
		return AvroFast::NONE;
	}
}

void HoldStrings(Vector &target, const vector<shared_ptr<RowBytes>> &pins) {
	switch (target.GetType().InternalType()) {
	case PhysicalType::VARCHAR:
		for (auto &pin : pins) {
			StringVector::AddAuxiliaryData(target, make_uniq<RowBytesHolder>(pin));
		}
		return;
	case PhysicalType::STRUCT:
		for (auto &child : StructVector::GetEntries(target)) {
			HoldStrings(child, pins);
		}
		return;
	case PhysicalType::LIST:
		HoldStrings(ListVector::GetChildMutable(target), pins);
		return;
	default:
		return;
	}
}

} // namespace

AvroNode AvroNode::Plan(const AvroType &type) {
	AvroNode node;
	node.kind = type.kind;
	const auto id = type.duckdb_type.id();
	switch (type.kind) {
	case AvroKind::NULL_TYPE:
		node.decode = AvroDecode::NULL_VALUE;
		break;
	case AvroKind::BOOLEAN:
		node.decode = AvroDecode::BOOLEAN;
		break;
	case AvroKind::INT:
		node.decode = id == LogicalTypeId::DATE   ? AvroDecode::DATE
		              : id == LogicalTypeId::TIME ? AvroDecode::TIME_MILLIS
		                                          : AvroDecode::INT;
		break;
	case AvroKind::LONG:
		node.decode = id == LogicalTypeId::TIME           ? AvroDecode::TIME_MICROS
		              : type.unit == AvroTimeUnit::MILLIS ? AvroDecode::TIMESTAMP_MILLIS
		                                                  : AvroDecode::LONG;
		break;
	case AvroKind::FLOAT:
		node.decode = AvroDecode::FLOAT;
		break;
	case AvroKind::DOUBLE:
		node.decode = AvroDecode::DOUBLE;
		break;
	case AvroKind::STRING:
		node.decode = id == LogicalTypeId::UUID ? AvroDecode::UUID_STRING : AvroDecode::STRING;
		break;
	case AvroKind::BYTES:
		node.decode = id == LogicalTypeId::DECIMAL ? AvroDecode::DECIMAL_BYTES : AvroDecode::BYTES;
		node.physical = type.duckdb_type.InternalType();
		break;
	case AvroKind::FIXED:
		node.size = type.fixed_size;
		node.physical = type.duckdb_type.InternalType();
		node.decode = id == LogicalTypeId::UUID       ? AvroDecode::UUID_FIXED
		              : id == LogicalTypeId::DECIMAL  ? AvroDecode::DECIMAL_FIXED
		              : id == LogicalTypeId::INTERVAL ? AvroDecode::DURATION
		                                              : AvroDecode::FIXED;
		break;
	case AvroKind::ENUM:
		node.decode = AvroDecode::ENUM;
		node.size = EnumType::GetSize(type.duckdb_type);
		node.physical = type.duckdb_type.InternalType();
		break;
	case AvroKind::RECORD:
		node.decode = id == LogicalTypeId::SQLNULL ? AvroDecode::EMPTY_RECORD : AvroDecode::RECORD;
		for (auto &child : type.children) {
			node.children.push_back(Plan(child.second));
		}
		break;
	case AvroKind::ARRAY:
		node.decode = id == LogicalTypeId::MAP ? AvroDecode::ARRAY_MAP : AvroDecode::ARRAY;
		node.children.push_back(Plan(type.children[0].second));
		break;
	case AvroKind::MAP:
		node.decode = AvroDecode::MAP;
		node.children.push_back(Plan(type.children[1].second));
		break;
	case AvroKind::UNION:
		node.decode = type.union_child_map.size() >= 2 ? AvroDecode::UNION : AvroDecode::NULLABLE;
		for (idx_t i = 0; i < type.children.size(); i++) {
			node.children.push_back(Plan(type.children[i].second));
			auto member = type.union_child_map.find(i);
			node.members.push_back(member == type.union_child_map.end() ? DConstants::INVALID_INDEX
			                                                            : member->second.GetIndex());
		}
		break;
	}
	node.strings = node.decode == AvroDecode::STRING || node.decode == AvroDecode::BYTES ||
	               node.decode == AvroDecode::FIXED || node.decode == AvroDecode::MAP;
	for (auto &child : node.children) {
		node.strings = node.strings || child.strings;
	}
	node.fast = FastOf(node.decode);
	if (node.decode == AvroDecode::NULLABLE && node.children.size() == 2) {
		const idx_t value = node.members[0] == DConstants::INVALID_INDEX ? 1 : 0;
		const idx_t null = 1 - value;
		if (node.members[value] != DConstants::INVALID_INDEX && node.children[null].decode == AvroDecode::NULL_VALUE &&
		    node.children[value].fast != AvroFast::NONE) {
			node.fast = node.children[value].fast;
			node.null_byte = static_cast<uint8_t>(AvroZigZagEncode(static_cast<int64_t>(null)));
			node.value_byte = static_cast<uint8_t>(AvroZigZagEncode(static_cast<int64_t>(value)));
		}
	}
	return node;
}

AvroSource::AvroSource(ClientContext &context, OpenFileInfo file_p) : file(std::move(file_p)) {
	CSVReaderOptions options;
	manager = CSVBufferManager::Open(context, options, file, false,
	                                 CSVFileHandle::OpenFile(context, file, options, CachingMode::ALWAYS_CACHE));
	input = make_uniq<RowInput>(context, *manager, file.path, true, &consumed);
	auto &allocator = BufferAllocator::Get(context);
	idx_t want = HEADER_READ_BYTES;
	while (true) {
		auto buffer = allocator.Allocate(want);
		const idx_t got = Rows().ReadFully(char_ptr_cast(buffer.get()), want, 0);
		if (AvroParseHeader(const_char_ptr_cast(buffer.get()), got, header)) {
			return;
		}
		if (got < want) {
			if (got < AVRO_MAGIC_SIZE) {
				throw InvalidInputException("Incorrect Avro container file magic number");
			}
			throw InvalidInputException("Avro file \"%s\" ends inside its header", file.path);
		}
		want *= 2;
	}
}

void AvroSource::ReadInOrder() {
	if (input->file) {
		input->file->in_order = true;
	}
}

double AvroSource::Progress() const {
	const idx_t size = input->host_stream ? 0 : manager->file_handle->FileSize();
	if (size == 0) {
		return 0;
	}
	const double position =
	    Stream() ? manager->file_handle->GetProgress() : static_cast<double>(header.size + consumed.load());
	return MinValue<double>(position / static_cast<double>(size), 1.0);
}

AvroReaderGlobal::AvroReaderGlobal(ClientContext &context_p, shared_ptr<AvroSource> source_p, const AvroType &root,
                                   AvroReaderColumns columns_p)
    : context(context_p), source(std::move(source_p)), columns(std::move(columns_p)) {
	const AvroType *record = &root;
	if (root.kind == AvroKind::UNION && root.union_child_map.size() == 1) {
		record_branch = root.union_child_map.begin()->first;
		record = &root.children[record_branch].second;
	}
	if (record->duckdb_type.id() == LogicalTypeId::STRUCT) {
		if (record != &root) {
			root_union = make_uniq<AvroNode>(AvroNode::Plan(root));
		}
		for (auto &child : record->children) {
			fields.push_back(AvroField {AvroNode::Plan(child.second)});
		}
	} else {
		fields.push_back(AvroField {AvroNode::Plan(root)});
	}
	vector<bool> filtered;
	for (auto &filter : columns.filters) {
		if (filter.output_column >= filtered.size()) {
			filtered.resize(filter.output_column + 1, false);
		}
		filtered[filter.output_column] = true;
	}
	for (auto &read : columns.reads) {
		auto &field = fields[read.file_column];
		field.output = read.output_column;
		const bool filter = read.output_column < filtered.size() && filtered[read.output_column];
		if (!columns.filters.empty() && !filter) {
			field.role = AvroFieldRole::DEFER;
			field.deferred = deferred++;
		} else {
			field.role = AvroFieldRole::DECODE;
		}
		walk = true;
	}
	const auto &header = source->header;
	idx_t range_bytes = DConstants::INVALID_INDEX;
	if (source->Stream()) {
		stream_bytes = RowInput::StreamBytes(source->HostStream());
		if (TaskScheduler::QueryThreads(context) > 1) {
			range_bytes = stream_bytes;
		}
	} else if (TaskScheduler::QueryThreads(context) > 1) {
		range_bytes = RowRanges::RangeBytes(source->Size(), TaskScheduler::QueryThreads(context), MIN_RANGE_BYTES,
		                                    MAX_RANGE_BYTES);
	}
	ranges =
	    make_uniq<RowRanges>(BufferAllocator::Get(context), source->Rows(), header.size, source->Size(), range_bytes);
	ranges->path = source->Path();
	if (!source->Stream() && ranges->RangeCount() > 1) {
		chain_first.resize(ranges->RangeCount(), DConstants::INVALID_INDEX);
		chain_end.resize(ranges->RangeCount(), DConstants::INVALID_INDEX);
	}
}

idx_t AvroReaderGlobal::FinishBlocks(idx_t range, idx_t first_block, idx_t blocks_end) {
	ranges->Finish(range);
	if (chain_first.empty()) {
		return DConstants::INVALID_INDEX;
	}
	lock_guard<mutex> guard(chain_lock);
	chain_first[range] = first_block;
	chain_end[range] = blocks_end;
	if (++chain_finished < chain_first.size()) {
		return DConstants::INVALID_INDEX;
	}
	idx_t expected = ranges->start;
	for (idx_t i = 0; i < chain_first.size(); i++) {
		if (chain_first[i] == DConstants::INVALID_INDEX) {
			continue;
		}
		if (chain_first[i] != expected) {
			return expected;
		}
		expected = chain_end[i];
	}
	return expected == ranges->Size() ? DConstants::INVALID_INDEX : expected;
}

idx_t AvroReaderGlobal::MaxThreads() const {
	if (ranges->Intake()) {
		return TaskScheduler::QueryThreads(context);
	}
	return ranges->RangeCount();
}

AvroReader::AvroReader(AvroReaderGlobal &global_p)
    : global(global_p), header(global_p.source->header), window(*global_p.ranges),
      decompressor(BufferAllocator::Get(global_p.context)) {
	for (auto &filter : global.columns.filters) {
		filter_states.push_back(TableFilterState::Initialize(global.context, filter.filter.get()));
	}
	filter_selection.Initialize(STANDARD_VECTOR_SIZE);
	positions.resize(global.deferred * STANDARD_VECTOR_SIZE);
	targets.resize(global.fields.size());
}

AvroReader::~AvroReader() {
	if (claimed && !loaded && global.ranges->Intake()) {
		global.ranges->FailIntake();
	}
}

bool AvroReader::Claim() {
	claimed = false;
	if (!global.Claim(range)) {
		return false;
	}
	claimed = true;
	loaded = false;
	return true;
}

idx_t AvroReader::Ensure(idx_t from, idx_t to) {
	if (global.ranges->Intake()) {
		return window.read_to;
	}
	while (window.read_to < to && window.read_to < global.ranges->Size()) {
		const idx_t before = window.read_to;
		const idx_t ahead = range_end > window.read_to ? MinValue(READ_BYTES, range_end - window.read_to) : 0;
		window.Read(from, MaxValue(to - window.read_to, ahead));
		if (window.read_to == before) {
			break;
		}
	}
	return window.read_to;
}

void AvroReader::Load() {
	loaded = true;
	auto &ranges = *global.ranges;
	block_left = 0;
	cursor = nullptr;
	block_end = nullptr;
	first_block = DConstants::INVALID_INDEX;
	blocks_end = DConstants::INVALID_INDEX;
	if (ranges.Intake()) {
		LoadIntake();
		return;
	}
	const idx_t begin = ranges.RangeBegin(range);
	range_end = ranges.RangeEnd(range);
	if (begin == ranges.start) {
		window.Start(begin, true);
		next_block = begin;
		searching = false;
		return;
	}
	window.Start(begin - AVRO_SYNC_SIZE, true);
	sync_from = begin - AVRO_SYNC_SIZE;
	searching = true;
	if (!FindSync(next_block)) {
		next_block = range_end;
	}
}

void AvroReader::LoadIntake() {
	auto &ranges = *global.ranges;
	idx_t from;
	if (!ranges.ClaimRead(range, from)) {
		claimed = false;
		return;
	}
	bool cut = false;
	try {
		window.StartAhead(from);
		while (window.read_to - from < global.stream_bytes && window.read_to < ranges.Size()) {
			window.Read(window.bytes->offset, global.stream_bytes - (window.read_to - from));
		}
		ranges.PassRead(window.read_to);
		RowClaim claim;
		if (!ranges.ClaimCut(range, claim)) {
			cut = true;
			claimed = false;
			return;
		}
		window.Prepend(std::move(claim.carry), claim.begin, from);
		idx_t end = CutBlocks(claim.begin);
		if (window.read_to >= ranges.Size()) {
			end = window.read_to;
		} else if (end == claim.begin) {
			ranges.CarryOn(std::move(window.bytes), window.read_to, RowSearch(), false);
			cut = true;
			window.read_to = claim.begin;
			global.Finish(range);
			claimed = false;
			return;
		}
		ranges.Cut(end, window.Tail(end), window.read_to, CSVState::RECORD_SEPARATOR, DConstants::INVALID_INDEX);
		cut = true;
		next_block = claim.begin;
		range_end = end;
		searching = false;
	} catch (...) {
		if (!cut) {
			ranges.FailIntake();
		}
		throw;
	}
}

idx_t AvroReader::CutBlocks(idx_t begin) const {
	idx_t end = begin;
	while (end < window.read_to) {
		const char *start = At(end);
		const char *pos = start;
		const char *limit = At(window.read_to);
		int64_t count;
		int64_t size;
		if (!AvroReadLong(pos, count) || pos > limit || !AvroReadLong(pos, size) || pos > limit) {
			return end;
		}
		if (count < 0 || size < 0) {
			return window.read_to;
		}
		const idx_t data_start = end + static_cast<idx_t>(pos - start);
		const auto length = static_cast<idx_t>(size);
		if (length > window.read_to - data_start || window.read_to - data_start - length < AVRO_SYNC_SIZE) {
			return end;
		}
		end = data_start + length + AVRO_SYNC_SIZE;
		if (memcmp(At(end - AVRO_SYNC_SIZE), header.sync, AVRO_SYNC_SIZE) != 0) {
			return end;
		}
	}
	return end;
}

bool AvroReader::FindSync(idx_t &position) {
	if (range_end < AVRO_SYNC_SIZE) {
		return false;
	}
	const idx_t last = range_end - AVRO_SYNC_SIZE;
	while (sync_from < last) {
		const idx_t available = Ensure(sync_from, MinValue(sync_from + READ_BYTES, range_end));
		if (available < sync_from + AVRO_SYNC_SIZE) {
			return false;
		}
		const idx_t stop = MinValue(available - AVRO_SYNC_SIZE + 1, last);
		const char *base = At(sync_from);
		const idx_t span = stop - sync_from;
		for (idx_t i = 0; i < span;) {
			auto candidate = static_cast<const char *>(memchr(base + i, header.sync[0], span - i));
			if (!candidate) {
				break;
			}
			i = static_cast<idx_t>(candidate - base);
			if (memcmp(candidate, header.sync, AVRO_SYNC_SIZE) == 0) {
				position = sync_from + i + AVRO_SYNC_SIZE;
				sync_from += i + 1;
				return true;
			}
			i++;
		}
		sync_from = stop;
		if (available >= global.ranges->Size()) {
			return false;
		}
	}
	return false;
}

bool AvroReader::ParseBlock(idx_t start, int64_t &count, idx_t &data_start, idx_t &size) {
	const idx_t available = Ensure(KeepFrom(start), start + 2 * AVRO_MAX_VARINT_SIZE);
	if (available <= start) {
		return false;
	}
	const char *begin = At(start);
	const char *pos = begin;
	const char *limit = At(available);
	int64_t raw_size;
	if (!AvroReadLong(pos, count) || pos > limit || !AvroReadLong(pos, raw_size) || pos > limit || count < 0 ||
	    raw_size < 0) {
		return false;
	}
	data_start = start + static_cast<idx_t>(pos - begin);
	size = static_cast<idx_t>(raw_size);
	if (size > global.ranges->Size() - data_start || global.ranges->Size() - data_start - size < AVRO_SYNC_SIZE) {
		return false;
	}
	const idx_t sync_end = data_start + size + AVRO_SYNC_SIZE;
	if (Ensure(KeepFrom(start), sync_end) < sync_end) {
		return false;
	}
	return memcmp(At(data_start + size), header.sync, AVRO_SYNC_SIZE) == 0;
}

shared_ptr<RowBytes> AvroReader::PoolBuffer() {
	for (auto &entry : pool) {
		if (entry.use_count() == 1) {
			return entry;
		}
	}
	pool.push_back(make_shared_ptr<RowBytes>());
	return pool.back();
}

void AvroReader::Pin(const shared_ptr<RowBytes> &bytes) {
	if (pins.empty() || pins.back() != bytes) {
		pins.push_back(bytes);
	}
}

void AvroReader::OpenBlock(idx_t data_start, idx_t size) {
	if (header.codec == AvroCodec::NONE) {
		cursor = At(data_start);
		block_end = cursor + size;
		block_bytes = window.bytes;
		Pin(block_bytes);
		return;
	}
	auto buffer = PoolBuffer();
	const idx_t length = decompressor.Decompress(header.codec, At(data_start), size, buffer->owned);
	buffer->data = const_char_ptr_cast(buffer->owned.get());
	buffer->size = length;
	buffer->capacity = buffer->owned.GetSize();
	cursor = buffer->data;
	block_end = cursor + length;
	block_bytes = std::move(buffer);
	Pin(block_bytes);
}

bool AvroReader::NextBlock() {
	while (true) {
		if (next_block >= range_end || next_block >= global.ranges->Size()) {
			return false;
		}
		int64_t count;
		idx_t data_start;
		idx_t size;
		if (!ParseBlock(next_block, count, data_start, size)) {
			if (searching) {
				if (!FindSync(next_block)) {
					next_block = range_end;
				}
				continue;
			}
			if (window.read_to <= next_block && next_block >= global.ranges->Size()) {
				return false;
			}
			block_offset = next_block;
			ThrowCorrupt("a block is truncated or does not end with the file's sync marker");
		}
		searching = false;
		block_offset = next_block;
		next_block = data_start + size + AVRO_SYNC_SIZE;
		if (first_block == DConstants::INVALID_INDEX) {
			first_block = block_offset;
		}
		blocks_end = next_block;
		global.source->Consume(next_block - block_offset);
		if (count == 0) {
			continue;
		}
		block_left = static_cast<idx_t>(count);
		record_index = 0;
		if (global.walk) {
			try {
				OpenBlock(data_start, size);
			} catch (const InvalidInputException &error) {
				ThrowCorrupt(ErrorData(error).RawMessage());
			}
		}
		return true;
	}
}

void AvroReader::ThrowCorrupt(const string &detail) const {
	throw InvalidInputException("Avro file \"%s\" is corrupt at the block at offset %llu: %s", global.source->Path(),
	                            block_offset, detail);
}

bool AvroReader::RootBranch(const char *&pos, const char *end) {
	if (!global.root_union) {
		return true;
	}
	const idx_t branch = ReadBranch(*global.root_union, pos, end);
	if (branch == global.record_branch) {
		return true;
	}
	Skip(global.root_union->children[branch], pos, end);
	return false;
}

void AvroReader::Walk(idx_t first, idx_t count, DataChunk &output) {
	const char *pos = cursor;
	const char *end = block_end;
	for (idx_t row = first; row < first + count; row++) {
		row_ends[row] = end;
		if (!RootBranch(pos, end)) {
			for (auto &field : global.fields) {
				if (field.role == AvroFieldRole::DECODE) {
					SetNull(output.data[field.output], row);
				} else if (field.role == AvroFieldRole::DEFER) {
					positions[field.deferred * STANDARD_VECTOR_SIZE + row] = nullptr;
				}
			}
			continue;
		}
		for (idx_t f = 0; f < global.fields.size(); f++) {
			auto &field = global.fields[f];
			switch (field.role) {
			case AvroFieldRole::SKIP:
				SkipFast(field.node, pos, end);
				break;
			case AvroFieldRole::DECODE:
				DecodeFast(field.node, pos, end, targets[f], row);
				break;
			case AvroFieldRole::DEFER:
				positions[field.deferred * STANDARD_VECTOR_SIZE + row] = pos;
				SkipFast(field.node, pos, end);
				break;
			}
		}
	}
	cursor = pos;
}

void AvroReader::Target(DataChunk &output, const AvroField &field) {
	auto &target = targets[&field - global.fields.data()];
	target.vector = &output.data[field.output];
	target.data = FlatVector::GetDataMutableUnsafe(*target.vector);
	target.validity = &FlatVector::ValidityMutable(*target.vector);
}

idx_t AvroReader::Fill(DataChunk &output) {
	const bool numbered = global.columns.file_row_number != DConstants::INVALID_INDEX;
	for (auto &field : global.fields) {
		if (field.role == AvroFieldRole::DECODE) {
			Target(output, field);
		}
	}
	idx_t rows = 0;
	while (rows < STANDARD_VECTOR_SIZE) {
		if (block_left == 0 && !NextBlock()) {
			break;
		}
		const idx_t take = MinValue(block_left, STANDARD_VECTOR_SIZE - rows);
		if (global.walk) {
			try {
				Walk(rows, take, output);
			} catch (const InvalidInputException &error) {
				ThrowCorrupt(ErrorData(error).RawMessage());
			}
		}
		if (numbered) {
			if (block_offset >= MAX_BLOCK_OFFSET || record_index + take > (idx_t(1) << RECORD_INDEX_BITS)) {
				throw InvalidInputException("Avro file \"%s\" has a block at offset %llu that file_row_number cannot "
				                            "address: blocks must start below %llu and hold at most %llu records",
				                            global.source->Path(), block_offset, MAX_BLOCK_OFFSET,
				                            idx_t(1) << RECORD_INDEX_BITS);
			}
			const auto base = static_cast<int64_t>(block_offset << RECORD_INDEX_BITS);
			for (idx_t i = 0; i < take; i++) {
				identities[rows + i] = base + static_cast<int64_t>(record_index + i);
			}
		}
		rows += take;
		block_left -= take;
		record_index += take;
		if (block_left == 0 && global.walk && cursor != block_end) {
			ThrowCorrupt("the records of a block do not fill its data");
		}
	}
	return rows;
}

idx_t AvroReader::ApplyFilters(DataChunk &output, idx_t rows, bool lookup) {
	auto &filters = global.columns.filters;
	idx_t approved = rows;
	filter_selection.Initialize(nullptr);
	for (idx_t i = 0; i < filters.size() && approved > 0; i++) {
		auto &vector = output.data[filters[i].output_column];
		ColumnSegment::FilterSelection(filter_selection, vector, *filter_states[i], rows, approved);
	}
	if (approved == rows) {
		return rows;
	}
	const auto slice = [&](Vector &vector) {
		vector.Slice(filter_selection, approved);
		vector.Flatten(approved);
	};
	for (auto &field : global.fields) {
		if (field.role == AvroFieldRole::DECODE || (lookup && field.role == AvroFieldRole::DEFER)) {
			slice(output.data[field.output]);
		}
	}
	if (global.columns.file_row_number != DConstants::INVALID_INDEX) {
		slice(output.data[global.columns.file_row_number]);
	}
	return approved;
}

idx_t AvroReader::Complete(DataChunk &output, idx_t rows) {
	for (auto &field : global.fields) {
		if (field.role != AvroFieldRole::SKIP && field.node.strings) {
			HoldStrings(output.data[field.output], pins);
		}
	}
	const idx_t numbers_column = global.columns.file_row_number;
	if (numbers_column != DConstants::INVALID_INDEX) {
		auto numbers = FlatVector::Writer<int64_t>(output.data[numbers_column], rows);
		for (idx_t i = 0; i < rows; i++) {
			numbers.WriteValue(identities[i]);
		}
	}
	output.SetChildCardinality(rows);
	idx_t count = rows;
	if (!global.columns.filters.empty()) {
		count = ApplyFilters(output, rows, false);
		if (count == 0) {
			return 0;
		}
	}
	const bool all = count == rows;
	if (global.deferred) {
		try {
			for (auto &field : global.fields) {
				if (field.role != AvroFieldRole::DEFER) {
					continue;
				}
				Target(output, field);
				auto &target = targets[&field - global.fields.data()];
				auto base = positions.data() + field.deferred * STANDARD_VECTOR_SIZE;
				for (idx_t i = 0; i < count; i++) {
					const idx_t row = all ? i : filter_selection.get_index(i);
					const char *pos = base[row];
					if (!pos) {
						SetNull(*target.vector, i);
						continue;
					}
					DecodeFast(field.node, pos, row_ends[row], target, i);
				}
			}
		} catch (const InvalidInputException &error) {
			ThrowCorrupt(ErrorData(error).RawMessage());
		}
	}
	output.SetChildCardinality(count);
	return count;
}

idx_t AvroReader::Scan(DataChunk &output) {
	while (claimed) {
		output.Reset();
		pins.clear();
		if (!loaded) {
			Load();
			if (!claimed) {
				return 0;
			}
		} else if (block_left != 0 && block_bytes) {
			Pin(block_bytes);
		}
		const idx_t rows = Fill(output);
		if (rows == 0) {
			const idx_t gap = global.FinishBlocks(range, first_block, blocks_end);
			claimed = false;
			if (gap != DConstants::INVALID_INDEX) {
				block_offset = gap;
				ThrowCorrupt("a block is truncated or does not end with the file's sync marker");
			}
			return 0;
		}
		const idx_t count = Complete(output, rows);
		if (count != 0) {
			return count;
		}
	}
	return 0;
}

bool AvroReader::LookupBlock(RowLookup &lookup, idx_t block, idx_t &records) {
	auto &ranges = *global.ranges;
	if (block < ranges.start || block >= ranges.Size()) {
		return false;
	}
	const auto found = lookup.Find(block, block, LOOKUP_BYTES);
	if (!found.IsValid()) {
		return false;
	}
	const idx_t base = found.GetIndex();
	if (lookup.End() - base < 2 * AVRO_MAX_VARINT_SIZE && !lookup.Eof()) {
		lookup.Extend(LOOKUP_BYTES);
	}
	const char *begin = lookup.bytes->data + base;
	const char *pos = begin;
	const char *limit = begin + (lookup.End() - base);
	int64_t count;
	int64_t size;
	if (!AvroReadLong(pos, count) || pos > limit || !AvroReadLong(pos, size) || pos > limit || count < 0 || size < 0) {
		return false;
	}
	const auto data_offset = static_cast<idx_t>(pos - begin);
	const idx_t length = static_cast<idx_t>(size);
	const idx_t total = data_offset + length + AVRO_SYNC_SIZE;
	while (lookup.End() - base < total && !lookup.Eof()) {
		lookup.Extend(total - (lookup.End() - base));
	}
	if (lookup.End() - base < total) {
		return false;
	}
	const char *data = lookup.bytes->data + base + data_offset;
	if (memcmp(data + length, header.sync, AVRO_SYNC_SIZE) != 0) {
		return false;
	}
	auto buffer = PoolBuffer();
	idx_t decoded = length;
	if (header.codec == AvroCodec::NONE) {
		if (buffer->owned.GetSize() < length + AVRO_PADDING) {
			buffer->owned = BufferAllocator::Get(global.context).Allocate(length + AVRO_PADDING);
		}
		memcpy(buffer->owned.get(), data, length);
		memset(buffer->owned.get() + length, 0, AVRO_PADDING);
	} else {
		decoded = decompressor.Decompress(header.codec, data, length, buffer->owned);
	}
	buffer->data = const_char_ptr_cast(buffer->owned.get());
	buffer->size = decoded;
	buffer->capacity = buffer->owned.GetSize();
	cursor = buffer->data;
	block_end = cursor + decoded;
	block_bytes = std::move(buffer);
	Pin(block_bytes);
	block_offset = block;
	records = static_cast<idx_t>(count);
	return true;
}

idx_t AvroReader::Lookup(std::span<const int64_t> rows, std::span<idx_t> survivors, DataChunk &output) {
	output.Reset();
	pins.clear();
	RowLookup lookup(*global.ranges);
	constexpr idx_t record_mask = (idx_t(1) << RECORD_INDEX_BITS) - 1;
	idx_t current = DConstants::INVALID_INDEX;
	idx_t records = 0;
	idx_t next_record = 0;
	idx_t found = 0;
	for (idx_t i = 0; i < rows.size(); i++) {
		if (rows[i] < 0) {
			continue;
		}
		const auto row = static_cast<idx_t>(rows[i]);
		const idx_t block = row >> RECORD_INDEX_BITS;
		const idx_t index = row & record_mask;
		if (block != current || index < next_record) {
			current = DConstants::INVALID_INDEX;
			try {
				if (!LookupBlock(lookup, block, records)) {
					continue;
				}
			} catch (const InvalidInputException &error) {
				block_offset = block;
				ThrowCorrupt(ErrorData(error).RawMessage());
			}
			current = block;
			next_record = 0;
		}
		if (index >= records) {
			continue;
		}
		try {
			for (; next_record < index; next_record++) {
				if (RootBranch(cursor, block_end)) {
					for (auto &field : global.fields) {
						Skip(field.node, cursor, block_end);
					}
				}
			}
			const bool present = RootBranch(cursor, block_end);
			for (auto &field : global.fields) {
				if (field.role == AvroFieldRole::SKIP) {
					if (present) {
						Skip(field.node, cursor, block_end);
					}
				} else if (present) {
					Decode(field.node, cursor, block_end, output.data[field.output], found);
				} else {
					SetNull(output.data[field.output], found);
				}
			}
		} catch (const InvalidInputException &error) {
			ThrowCorrupt(ErrorData(error).RawMessage());
		}
		next_record++;
		identities[found] = rows[i];
		survivors[found] = i;
		found++;
	}
	for (auto &field : global.fields) {
		if (field.role != AvroFieldRole::SKIP && field.node.strings) {
			HoldStrings(output.data[field.output], pins);
		}
	}
	const idx_t numbers_column = global.columns.file_row_number;
	if (numbers_column != DConstants::INVALID_INDEX) {
		auto numbers = FlatVector::Writer<int64_t>(output.data[numbers_column], found);
		for (idx_t r = 0; r < found; r++) {
			numbers.WriteValue(identities[r]);
		}
	}
	output.SetChildCardinality(found);
	if (!global.columns.filters.empty() && found != 0) {
		const idx_t approved = ApplyFilters(output, found, true);
		for (idx_t r = 0; r < approved && approved != found; r++) {
			survivors[r] = survivors[filter_selection.get_index(r)];
		}
		found = approved;
		output.SetChildCardinality(found);
	}
	return found;
}

} // namespace duckdb
