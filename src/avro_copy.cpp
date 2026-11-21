#include "avro_copy.hpp"

#include "avro_binary.hpp"
#include "field_ids.hpp"
#include "read_avro.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/serializer/async_file_writer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

#include <algorithm>

namespace duckdb {

namespace {

constexpr idx_t AVRO_BLOCK_SIZE = 256 * 1024;
constexpr idx_t AVRO_UUID_SIZE = 16;
constexpr uint8_t AVRO_NULL_BRANCH = 0;
constexpr uint8_t AVRO_VALUE_BRANCH = 2;
constexpr uint8_t AVRO_ARRAY_END = 0;

string ConvertTypeToAvro(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::BLOB:
		return "bytes";
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BIGINT:
		return "long";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::SQLNULL:
		return "null";
	case LogicalTypeId::STRUCT:
		return "record";
	case LogicalTypeId::DATE:
		return "int";
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return "long";
	case LogicalTypeId::UUID:
	case LogicalTypeId::DECIMAL:
		return "fixed";
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return "array";
	default:
		throw NotImplementedException("Can't convert logical type '%s' to Avro type", type.ToString());
	}
}

string GetTemporalLogicalType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME:
		return "time-micros";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamp-micros";
	case LogicalTypeId::TIMESTAMP_NS:
		return "timestamp-nanos";
	default:
		throw NotImplementedException("Can't convert logical type '%s' to Avro temporal type", type.ToString());
	}
}

uint32_t MinBytesRequiredForDecimal(int32_t precision) {
	static constexpr int32_t BITS_REQUIRED[] = {0,  4,  7,  10, 14,  17,  20,  24,  27,  30,  34,  37,  40,
	                                            44, 47, 50, 54, 57,  60,  64,  67,  70,  74,  77,  80,  84,
	                                            87, 90, 94, 97, 100, 103, 107, 110, 113, 117, 120, 123, 127};
	return (BITS_REQUIRED[precision] + 1 + 7) / 8;
}

bool IsNamedSchema(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UUID:
	case LogicalTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

bool IsAvroIdentifier(const string &name) {
	if (name.empty()) {
		return false;
	}
	for (idx_t i = 0; i < name.size(); i++) {
		const auto c = name[i];
		if (!(StringUtil::CharacterIsAlpha(c) || c == '_' || (i && StringUtil::CharacterIsDigit(c)))) {
			return false;
		}
	}
	return true;
}

struct AvroWriteSchema {
	LogicalType type;
	bool nullable = false;
	string field;
	string name;
	bool has_field_id = false;
	int32_t field_id = 0;
	idx_t entry = 0;
	vector<AvroWriteSchema> children;
};

void WriteAvroValueType(JSONStringWriter &writer, const AvroWriteSchema &schema);

void WriteAvroType(JSONStringWriter &writer, const AvroWriteSchema &schema) {
	if (!schema.nullable) {
		WriteAvroValueType(writer, schema);
		return;
	}
	writer.StartArray();
	writer.String("null");
	WriteAvroValueType(writer, schema);
	writer.EndArray();
}

void WriteAvroField(JSONStringWriter &writer, const AvroWriteSchema &field) {
	writer.StartObject();
	writer.Key("name");
	writer.String(field.field);
	writer.Key("type");
	WriteAvroType(writer, field);
	if (field.has_field_id) {
		writer.Key("field-id");
		writer.SignedInteger(field.field_id);
	}
	writer.EndObject();
}

void WriteAvroValueType(JSONStringWriter &writer, const AvroWriteSchema &schema) {
	auto &type = schema.type;
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		writer.StartObject();
		writer.Key("type");
		writer.String("record");
		writer.Key("name");
		writer.String(schema.name);
		writer.Key("fields");
		writer.StartArray();
		for (auto &field : schema.children) {
			WriteAvroField(writer, field);
		}
		writer.EndArray();
		writer.EndObject();
		break;
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP: {
		auto &items = schema.children[0];
		writer.StartObject();
		writer.Key("type");
		writer.String("array");
		if (items.has_field_id) {
			writer.Key("element-id");
			writer.SignedInteger(items.field_id);
		}
		if (type.id() == LogicalTypeId::MAP) {
			writer.Key("logicalType");
			writer.String("map");
		}
		writer.Key("items");
		WriteAvroType(writer, items);
		writer.EndObject();
		break;
	}
	case LogicalTypeId::DECIMAL: {
		const auto width = DecimalType::GetWidth(type);
		writer.StartObject();
		writer.Key("type");
		writer.String("fixed");
		writer.Key("name");
		writer.String(schema.name);
		writer.Key("size");
		writer.UnsignedInteger(MinBytesRequiredForDecimal(width));
		writer.Key("logicalType");
		writer.String("decimal");
		writer.Key("precision");
		writer.UnsignedInteger(width);
		writer.Key("scale");
		writer.UnsignedInteger(DecimalType::GetScale(type));
		writer.EndObject();
		break;
	}
	case LogicalTypeId::UUID:
		writer.StartObject();
		writer.Key("type");
		writer.String("fixed");
		writer.Key("name");
		writer.String(schema.name);
		writer.Key("size");
		writer.UnsignedInteger(AVRO_UUID_SIZE);
		writer.Key("logicalType");
		writer.String("uuid");
		writer.EndObject();
		break;
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		writer.StartObject();
		writer.Key("type");
		writer.String(ConvertTypeToAvro(type));
		writer.Key("logicalType");
		writer.String(GetTemporalLogicalType(type));
		if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			writer.Key("adjust-to-utc");
			writer.Boolean(true);
		}
		writer.EndObject();
		break;
	default:
		writer.String(ConvertTypeToAvro(type));
		break;
	}
}

struct JSONSchemaGenerator {
private:
	struct MapKeyValueIds {
		int64_t key_id;
		int64_t value_id;
	};

public:
	JSONSchemaGenerator(const vector<string> &names, const vector<LogicalType> &types) : names(names), types(types) {
	}

public:
	void ParseFieldIds(const identifier_map_t<vector<Value>> &options, identifier_set_t &recognized) {
		auto it = options.find("FIELD_IDS");
		if (it == options.end()) {
			return;
		}
		if (it->second.empty()) {
			throw InvalidInputException("FIELD_IDS can not be provided without a value");
		}
		field_ids = avro::FieldIDUtils::ParseFieldIds(it->second[0], names, types);
		recognized.insert(it->first);
	}

	void ParseRootName(const identifier_map_t<vector<Value>> &options, identifier_set_t &recognized) {
		auto it = options.find("ROOT_NAME");
		if (it == options.end()) {
			return;
		}
		if (it->second.empty()) {
			throw InvalidInputException("ROOT_NAME can not be provided without a value");
		}
		auto &value = it->second[0];
		if (value.type().id() != LogicalTypeId::VARCHAR) {
			throw InvalidInputException("'ROOT_NAME' is expected to be provided as VARCHAR, this is used for the name "
			                            "of the top level 'record'");
		}
		root_name = value.GetValue<string>();
		recognized.insert(it->first);
	}

	void ParseSanitizeFieldNames(const identifier_map_t<vector<Value>> &options, identifier_set_t &recognized) {
		auto it = options.find("SANITIZE_FIELD_NAMES");
		if (it == options.end()) {
			return;
		}
		if (it->second.size() != 1 || it->second[0].IsNull() || it->second[0].type().id() != LogicalTypeId::BOOLEAN) {
			throw InvalidInputException("SANITIZE_FIELD_NAMES requires a non-NULL BOOLEAN value");
		}
		sanitize_field_names = it->second[0].GetValue<bool>();
		recognized.insert(it->first);
	}

	AvroWriteSchema Generate() {
		VerifyAvroName(root_name);
		VerifyNamedSchemaUniqueness(root_name);
		D_ASSERT(names.size() == types.size());
		AvroWriteSchema root;
		root.type = LogicalType(LogicalTypeId::STRUCT);
		root.name = root_name;
		unordered_map<string, string> field_names;
		for (idx_t i = 0; i < names.size(); i++) {
			optional_ptr<avro::FieldID> field_id;
			auto &children = field_ids.Ids();
			auto it = children.find(names[i]);
			if (it != children.end()) {
				field_id = it->second;
			}
			root.children.push_back(CreateStructField(names[i], types[i], field_id, field_names));
			root.children.back().entry = i;
		}
		return root;
	}

private:
	void VerifyAvroName(const string &name) {
		if (!IsAvroIdentifier(name)) {
			throw InvalidInputException("'%s' is not a valid Avro identifier\nThe identifier has to match the "
			                            "regex: [A-Za-z_][A-Za-z0-9_]*",
			                            name);
		}
	}

	void VerifyNamedSchemaUniqueness(const string &name) {
		auto res = named_schemas.insert(name);
		if (!res.second) {
			throw BinderException("Avro schema by the name of '%s' already exists, names of 'record', 'enum' and "
			                      "'fixed' types have to be distinct",
			                      name);
		}
	}

	string GenerateSchemaName(const string &base) {
		auto res = StringUtil::Format("%s%d", base, generated_name_id++);
		VerifyAvroName(res);
		VerifyNamedSchemaUniqueness(res);
		return res;
	}

	string SanitizeFieldName(const string &name, unordered_map<string, string> &field_names) {
		if (!sanitize_field_names) {
			return name;
		}
		if (name.empty()) {
			throw InvalidInputException("Cannot sanitize an empty Avro field name");
		}
		string result;
		result.reserve(name.size());
		for (idx_t i = 0; i < name.size(); i++) {
			auto c = static_cast<unsigned char>(name[i]);
			auto digit = c >= '0' && c <= '9';
			if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (i && digit)) {
				result += c;
			} else if (digit) {
				result += '_';
				result += c;
			} else {
				result += StringUtil::Format("_x%X", static_cast<unsigned int>(c));
			}
		}
		auto entry = field_names.emplace(result, name);
		if (!entry.second) {
			throw BinderException("SANITIZE_FIELD_NAMES maps both '%s' and '%s' to '%s' in the same Avro record",
			                      entry.first->second, name, result);
		}
		return result;
	}

	static bool HasId(optional_ptr<avro::FieldID> field_id) {
		return field_id && field_id->set;
	}

	static void ApplyFieldId(AvroWriteSchema &schema, optional_ptr<avro::FieldID> field_id) {
		schema.nullable = schema.type.id() != LogicalTypeId::SQLNULL && (!field_id || field_id->nullable);
		if (HasId(field_id)) {
			schema.has_field_id = true;
			schema.field_id = field_id->GetFieldId();
		}
	}

	AvroWriteSchema CreateStructField(const string &name, const LogicalType &type, optional_ptr<avro::FieldID> field_id,
	                                  unordered_map<string, string> &field_names) {
		auto output_name = SanitizeFieldName(name, field_names);
		auto schema_name = output_name;
		if (type.id() == LogicalTypeId::STRUCT && HasId(field_id)) {
			schema_name = StringUtil::Format("r%d", field_id->GetFieldId());
		}
		auto result = CreateSchemaType(type, field_id, schema_name.c_str());
		ApplyFieldId(result, field_id);
		result.field = std::move(output_name);
		return result;
	}

	AvroWriteSchema CreateSchemaType(const LogicalType &type, optional_ptr<avro::FieldID> field_id,
	                                 const char *preset_schema_name = nullptr) {
		auto avro_type_str = ConvertTypeToAvro(type);
		AvroWriteSchema result;
		result.type = type;
		switch (type.id()) {
		case LogicalTypeId::STRUCT: {
			auto &struct_children = StructType::GetChildTypes(type);
			unordered_map<string, string> field_names;
			for (idx_t i = 0; i < struct_children.size(); i++) {
				auto &child = struct_children[i];
				if (child.first == "__duckdb_empty_struct_marker") {
					continue;
				}
				auto &child_name = child.first.GetIdentifierName();
				auto child_field_id = GetChildFieldIdByName(field_id, child_name);
				result.children.push_back(CreateStructField(child_name, child.second, child_field_id, field_names));
				result.children.back().entry = i;
			}
			break;
		}
		case LogicalTypeId::LIST: {
			auto &list_child = ListType::GetChildType(type);
			auto element_field_id = GetChildFieldIdByName(field_id, "list");
			AvroWriteSchema items;
			if (list_child.id() == LogicalTypeId::STRUCT && HasId(element_field_id)) {
				auto preset = StringUtil::Format("r%d", element_field_id->GetFieldId());
				items = CreateSchemaType(list_child, element_field_id, preset.c_str());
			} else {
				items = CreateSchemaType(list_child, element_field_id);
			}
			ApplyFieldId(items, element_field_id);
			result.children.push_back(std::move(items));
			break;
		}
		case LogicalTypeId::MAP: {
			auto &list_child = ListType::GetChildType(type);
			D_ASSERT(list_child.id() == LogicalTypeId::STRUCT);
			MapKeyValueIds key_value_ids;
			if (GetMapKeyValueIds(field_id, key_value_ids)) {
				auto preset = StringUtil::Format("k%d_v%d", key_value_ids.key_id, key_value_ids.value_id);
				result.children.push_back(CreateSchemaType(list_child, field_id, preset.c_str()));
			} else {
				result.children.push_back(CreateSchemaType(list_child, field_id));
			}
			break;
		}
		default:
			break;
		}
		if (IsNamedSchema(type)) {
			if (preset_schema_name) {
				VerifyAvroName(preset_schema_name);
				VerifyNamedSchemaUniqueness(preset_schema_name);
				result.name = preset_schema_name;
			} else {
				result.name = GenerateSchemaName(avro_type_str);
			}
		}
		return result;
	}

	optional_ptr<avro::FieldID> GetChildFieldIdByName(optional_ptr<avro::FieldID> parent, const string &name) {
		if (!parent) {
			return nullptr;
		}
		auto &children = parent->children.Ids();
		auto it = children.find(name);
		if (it != children.end()) {
			return it->second;
		}
		return nullptr;
	}

	bool GetMapKeyValueIds(optional_ptr<avro::FieldID> field_id, MapKeyValueIds &ids) {
		if (!field_id) {
			return false;
		}
		auto key = GetChildFieldIdByName(field_id, "key");
		auto value = GetChildFieldIdByName(field_id, "value");
		if (HasId(key) && HasId(value)) {
			ids.key_id = key->GetFieldId();
			ids.value_id = value->GetFieldId();
			return true;
		}
		return false;
	}

private:
	const vector<string> &names;
	const vector<LogicalType> &types;

	bool sanitize_field_names = false;
	string root_name = "root";
	avro::ChildFieldIDs field_ids;
	idx_t generated_name_id = 0;
	unordered_set<string> named_schemas;
};

string CreateJSONSchema(const AvroWriteSchema &root) {
	try {
		JSONStringWriter writer(JSONWriteFlags::ALLOW_INF_AND_NAN);
		WriteAvroValueType(writer, root);
		return writer.ToString();
	} catch (SerializationException &) {
		throw InvalidInputException("Could not create a JSON representation of the table schema");
	}
}

void VerifyFieldNames(const AvroWriteSchema &schema) {
	for (auto &child : schema.children) {
		if (schema.type.id() == LogicalTypeId::STRUCT && !IsAvroIdentifier(child.field)) {
			throw InvalidInputException("Invalid Avro identifier \"%s\": field names have to match the regex "
			                            "[A-Za-z_][A-Za-z0-9_]*, or set SANITIZE_FIELD_NAMES",
			                            child.field);
		}
		VerifyFieldNames(child);
	}
}

vector<AvroMetadataEntry> ParseMetadata(const identifier_map_t<vector<Value>> &options, identifier_set_t &recognized) {
	vector<AvroMetadataEntry> result;
	auto it = options.find("METADATA");
	if (it == options.end()) {
		return result;
	}
	if (it->second.empty()) {
		throw InvalidInputException("METADATA can not be provided without a value");
	}
	auto &value = it->second[0];
	if (value.type().id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("'METADATA' is expected to be provided as a STRUCT of key-value string metadata");
	}
	recognized.insert(it->first);
	auto &children = StructValue::GetChildren(value);
	auto &child_types = StructType::GetChildTypes(value.type());
	for (idx_t i = 0; i < children.size(); i++) {
		result.push_back(AvroMetadataEntry {child_types[i].first.GetIdentifierName(), children[i].ToString()});
	}
	return result;
}

string ParseCodec(const identifier_map_t<vector<Value>> &options, identifier_set_t &recognized) {
	auto it = options.find("CODEC");
	if (it == options.end()) {
		return "";
	}
	if (it->second.empty()) {
		throw InvalidInputException("CODEC can not be provided without a value");
	}
	auto &value = it->second[0];
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("'CODEC' is expected to be provided as VARCHAR (e.g. 'deflate', 'null')");
	}
	recognized.insert(it->first);
	return StringUtil::Lower(value.GetValue<string>());
}

void VerifyRecognizedOptions(const identifier_map_t<vector<Value>> &options, const identifier_set_t &recognized) {
	vector<string> unrecognized_options;
	for (auto &option : options) {
		if (recognized.count(option.first)) {
			continue;
		}
		auto key = option.first;
		if (option.second.empty()) {
			unrecognized_options.push_back(StringUtil::Format("key: %s", key));
		} else {
			unrecognized_options.push_back(
			    StringUtil::Format("key: %s with value: '%s'", key, option.second[0].ToString()));
		}
	}
	if (!unrecognized_options.empty()) {
		throw InvalidConfigurationException("The following option(s) are not recognized: %s",
		                                    StringUtil::Join(unrecognized_options, ", "));
	}
}

struct WriteAvroBindData : public FunctionData {
public:
	WriteAvroBindData(vector<AvroWriteSchema> columns_p, vector<AvroMetadataEntry> metadata_p, AvroCodec codec_p)
	    : columns(std::move(columns_p)), metadata(std::move(metadata_p)), codec(codec_p) {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<WriteAvroBindData>(columns, metadata, codec);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<WriteAvroBindData>();
		if (metadata.size() != other.metadata.size()) {
			return false;
		}
		for (idx_t i = 0; i < metadata.size(); i++) {
			if (metadata[i].key != other.metadata[i].key || metadata[i].value != other.metadata[i].value) {
				return false;
			}
		}
		return true;
	}

public:
	vector<AvroWriteSchema> columns;
	vector<AvroMetadataEntry> metadata;
	AvroCodec codec;
};

int64_t AvroLongValue(int32_t value) {
	return value;
}

int64_t AvroLongValue(int64_t value) {
	return value;
}

int64_t AvroLongValue(date_t value) {
	return Date::EpochDays(value);
}

int64_t AvroLongValue(dtime_t value) {
	return value.value;
}

int64_t AvroLongValue(timestamp_t value) {
	return value.value;
}

int64_t AvroLongValue(timestamp_tz_t value) {
	return value.value;
}

int64_t AvroLongValue(timestamp_ns_t value) {
	return value.value;
}

int64_t AvroLongValue(timestamp_ms_t value) {
	return value.IsFinite() ? Timestamp::FromEpochMs(value.value).value : value.value;
}

struct AvroBooleanOp {
	idx_t MaxSize() const {
		return 1;
	}
	data_ptr_t Write(bool value, data_ptr_t out) const {
		*out = value ? 1 : 0;
		return out + 1;
	}
};

template <class T>
struct AvroLongOp {
	idx_t MaxSize() const {
		return AVRO_MAX_VARINT_SIZE;
	}
	data_ptr_t Write(const T &value, data_ptr_t out) const {
		return out + AvroWriteLong(AvroLongValue(value), out);
	}
};

template <class T>
struct AvroFloatOp {
	idx_t MaxSize() const {
		return sizeof(T);
	}
	data_ptr_t Write(T value, data_ptr_t out) const {
		const auto bytes = BSwapIfBE(value);
		memcpy(out, &bytes, sizeof(bytes));
		return out + sizeof(bytes);
	}
};

struct AvroBytesOp {
	data_ptr_t Write(const string_t &value, data_ptr_t out) const {
		const auto size = value.GetSize();
		out += AvroWriteLong(UnsafeNumericCast<int64_t>(size), out);
		memcpy(out, value.GetData(), size);
		return out + size;
	}
};

struct AvroUUIDOp {
	idx_t MaxSize() const {
		return AVRO_UUID_SIZE;
	}
	data_ptr_t Write(hugeint_t value, data_ptr_t out) const {
		BaseUUID::ToBlob(value, out);
		return out + AVRO_UUID_SIZE;
	}
};

template <class T>
struct AvroDecimalOp {
	idx_t size;

	idx_t MaxSize() const {
		return size;
	}
	data_ptr_t Write(const T &value, data_ptr_t out) const {
		if constexpr (std::is_same_v<T, hugeint_t>) {
			const uint64_t bytes[] = {BSwapIfLE(static_cast<uint64_t>(value.upper)), BSwapIfLE(value.lower)};
			memcpy(out, const_data_ptr_cast(bytes) + sizeof(bytes) - size, size);
		} else {
			const auto bytes = BSwapIfLE(static_cast<uint64_t>(static_cast<int64_t>(value)));
			memcpy(out, const_data_ptr_cast(&bytes) + sizeof(bytes) - size, size);
		}
		return out + size;
	}
};

class AvroColumnEncoder {
public:
	AvroColumnEncoder(Allocator &allocator, const AvroWriteSchema &schema)
	    : type(schema.type), nullable(schema.nullable), buffer(allocator) {
	}
	virtual ~AvroColumnEncoder() = default;

	virtual void Encode(const Vector &vector, idx_t count) = 0;

	virtual const LogicalType &RejectedType(idx_t row) const {
		return type;
	}

	idx_t Size() const {
		return ends.empty() ? 0 : ends.back();
	}

	idx_t RowsSize(idx_t begin, idx_t end) const {
		return ends[end - 1] - RowStart(begin);
	}

	data_ptr_t CopyRows(idx_t begin, idx_t end, data_ptr_t out) const {
		const auto start = RowStart(begin);
		const auto size = ends[end - 1] - start;
		memcpy(out, encoded + start, size);
		return out + size;
	}

	bool Rejects(idx_t row) const {
		return std::binary_search(rejected.begin(), rejected.end(), row);
	}

public:
	unsafe_vector<idx_t> rejected;

protected:
	data_ptr_t Start(idx_t count, idx_t capacity) {
		ends.resize(count);
		rejected.clear();
		buffer.GrowCapacity(capacity);
		encoded = buffer.GetData();
		return encoded;
	}

	idx_t RowStart(idx_t row) const {
		return row ? ends[row - 1] : 0;
	}

protected:
	const LogicalType type;
	const bool nullable;
	unsafe_vector<idx_t> ends;
	UnifiedVectorFormat format;

private:
	MemoryStream buffer;
	data_ptr_t encoded = nullptr;
};

unique_ptr<AvroColumnEncoder> CreateAvroEncoder(Allocator &allocator, const AvroWriteSchema &schema);

template <class T, class OP>
class AvroValueEncoder final : public AvroColumnEncoder {
public:
	AvroValueEncoder(Allocator &allocator, const AvroWriteSchema &schema, OP op_p = OP())
	    : AvroColumnEncoder(allocator, schema), op(op_p) {
	}

	void Encode(const Vector &vector, idx_t count) override {
		vector.ToUnifiedFormat(format);
		const auto values = format.GetData<T>();
		const auto base = Start(count, Capacity(values, count));
		const auto row_ends = ends.data();
		const auto sel = format.sel;
		const auto &validity = format.validity;
		const auto is_nullable = nullable;
		auto out = base;
		for (idx_t row = 0; row < count; row++) {
			const auto idx = sel->get_index(row);
			if (validity.RowIsValid(idx)) {
				if (is_nullable) {
					*out++ = AVRO_VALUE_BRANCH;
				}
				out = op.Write(values[idx], out);
			} else if (is_nullable) {
				*out++ = AVRO_NULL_BRANCH;
			} else {
				rejected.push_back(row);
			}
			row_ends[row] = UnsafeNumericCast<idx_t>(out - base);
		}
	}

private:
	idx_t Capacity(const T *values, idx_t count) const {
		if constexpr (std::is_same_v<T, string_t>) {
			auto result = count * (1 + AVRO_MAX_VARINT_SIZE);
			for (idx_t row = 0; row < count; row++) {
				const auto idx = format.sel->get_index(row);
				if (format.validity.RowIsValid(idx)) {
					result += values[idx].GetSize();
				}
			}
			return result;
		} else {
			return count * (1 + op.MaxSize());
		}
	}

private:
	OP op;
};

class AvroNullEncoder final : public AvroColumnEncoder {
public:
	using AvroColumnEncoder::AvroColumnEncoder;

	void Encode(const Vector &, idx_t count) override {
		const auto base = Start(count, count);
		const auto row_ends = ends.data();
		auto out = base;
		for (idx_t row = 0; row < count; row++) {
			if (nullable) {
				*out++ = AVRO_NULL_BRANCH;
			}
			row_ends[row] = UnsafeNumericCast<idx_t>(out - base);
		}
	}
};

class AvroStructEncoder final : public AvroColumnEncoder {
public:
	AvroStructEncoder(Allocator &allocator, const AvroWriteSchema &schema) : AvroColumnEncoder(allocator, schema) {
		for (auto &child : schema.children) {
			entries.push_back(child.entry);
			children.push_back(CreateAvroEncoder(allocator, child));
		}
	}

	void Encode(const Vector &vector, idx_t count) override {
		vector.ToUnifiedFormat(format);
		auto &child_vectors = StructVector::GetEntries(vector);
		auto capacity = count;
		auto children_rejected = false;
		for (idx_t i = 0; i < children.size(); i++) {
			children[i]->Encode(child_vectors[entries[i]], count);
			capacity += children[i]->Size();
			children_rejected = children_rejected || !children[i]->rejected.empty();
		}
		const auto base = Start(count, capacity);
		const auto row_ends = ends.data();
		const auto sel = format.sel;
		const auto &validity = format.validity;
		const auto is_nullable = nullable;
		auto out = base;
		for (idx_t row = 0; row < count; row++) {
			const auto idx = sel->get_index(row);
			if (validity.RowIsValid(idx)) {
				if (is_nullable) {
					*out++ = AVRO_VALUE_BRANCH;
				}
				for (auto &child : children) {
					out = child->CopyRows(row, row + 1, out);
				}
				if (children_rejected && RejectingChild(row)) {
					rejected.push_back(row);
				}
			} else if (is_nullable) {
				*out++ = AVRO_NULL_BRANCH;
			} else {
				rejected.push_back(row);
			}
			row_ends[row] = UnsafeNumericCast<idx_t>(out - base);
		}
	}

	const LogicalType &RejectedType(idx_t row) const override {
		if (format.validity.RowIsValid(format.sel->get_index(row))) {
			auto child = RejectingChild(row);
			if (child) {
				return child->RejectedType(row);
			}
		}
		return type;
	}

private:
	optional_ptr<const AvroColumnEncoder> RejectingChild(idx_t row) const {
		for (auto &child : children) {
			if (child->Rejects(row)) {
				return child.get();
			}
		}
		return nullptr;
	}

private:
	unsafe_vector<idx_t> entries;
	vector<unique_ptr<AvroColumnEncoder>> children;
};

class AvroListEncoder final : public AvroColumnEncoder {
public:
	AvroListEncoder(Allocator &allocator, const AvroWriteSchema &schema)
	    : AvroColumnEncoder(allocator, schema), items(CreateAvroEncoder(allocator, schema.children[0])) {
	}

	void Encode(const Vector &vector, idx_t count) override {
		vector.ToUnifiedFormat(format);
		items->Encode(ListVector::GetChild(vector), ListVector::GetListSize(vector));
		const auto lists = format.GetData<list_entry_t>();
		const auto base = Start(count, Capacity(lists, count));
		const auto row_ends = ends.data();
		const auto sel = format.sel;
		const auto &validity = format.validity;
		const auto is_nullable = nullable;
		const auto items_rejected = !items->rejected.empty();
		auto out = base;
		for (idx_t row = 0; row < count; row++) {
			const auto idx = sel->get_index(row);
			if (validity.RowIsValid(idx)) {
				if (is_nullable) {
					*out++ = AVRO_VALUE_BRANCH;
				}
				const auto &list = lists[idx];
				if (list.length) {
					out += AvroWriteLong(UnsafeNumericCast<int64_t>(list.length), out);
					out = items->CopyRows(list.offset, list.offset + list.length, out);
					if (items_rejected && RejectedItem(list).IsValid()) {
						rejected.push_back(row);
					}
				}
				*out++ = AVRO_ARRAY_END;
			} else if (is_nullable) {
				*out++ = AVRO_NULL_BRANCH;
			} else {
				rejected.push_back(row);
			}
			row_ends[row] = UnsafeNumericCast<idx_t>(out - base);
		}
	}

	const LogicalType &RejectedType(idx_t row) const override {
		const auto idx = format.sel->get_index(row);
		if (format.validity.RowIsValid(idx)) {
			auto item = RejectedItem(format.GetData<list_entry_t>()[idx]);
			if (item.IsValid()) {
				return items->RejectedType(item.GetIndex());
			}
		}
		return type;
	}

private:
	idx_t Capacity(const list_entry_t *lists, idx_t count) const {
		auto result = count;
		for (idx_t row = 0; row < count; row++) {
			const auto idx = format.sel->get_index(row);
			if (!format.validity.RowIsValid(idx)) {
				continue;
			}
			const auto &list = lists[idx];
			result += AVRO_MAX_VARINT_SIZE + 1;
			if (list.length) {
				result += items->RowsSize(list.offset, list.offset + list.length);
			}
		}
		return result;
	}

	optional_idx RejectedItem(const list_entry_t &list) const {
		auto &item_rows = items->rejected;
		auto item = std::lower_bound(item_rows.begin(), item_rows.end(), list.offset);
		if (item == item_rows.end() || *item >= list.offset + list.length) {
			return optional_idx();
		}
		return optional_idx(*item);
	}

private:
	unique_ptr<AvroColumnEncoder> items;
};

template <class T>
unique_ptr<AvroColumnEncoder> CreateAvroDecimalEncoder(Allocator &allocator, const AvroWriteSchema &schema) {
	AvroDecimalOp<T> op {MinBytesRequiredForDecimal(DecimalType::GetWidth(schema.type))};
	return make_uniq<AvroValueEncoder<T, AvroDecimalOp<T>>>(allocator, schema, op);
}

unique_ptr<AvroColumnEncoder> CreateAvroEncoder(Allocator &allocator, const AvroWriteSchema &schema) {
	auto &type = schema.type;
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return make_uniq<AvroValueEncoder<bool, AvroBooleanOp>>(allocator, schema);
	case LogicalTypeId::INTEGER:
		return make_uniq<AvroValueEncoder<int32_t, AvroLongOp<int32_t>>>(allocator, schema);
	case LogicalTypeId::BIGINT:
		return make_uniq<AvroValueEncoder<int64_t, AvroLongOp<int64_t>>>(allocator, schema);
	case LogicalTypeId::FLOAT:
		return make_uniq<AvroValueEncoder<float, AvroFloatOp<float>>>(allocator, schema);
	case LogicalTypeId::DOUBLE:
		return make_uniq<AvroValueEncoder<double, AvroFloatOp<double>>>(allocator, schema);
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return make_uniq<AvroValueEncoder<string_t, AvroBytesOp>>(allocator, schema);
	case LogicalTypeId::DATE:
		return make_uniq<AvroValueEncoder<date_t, AvroLongOp<date_t>>>(allocator, schema);
	case LogicalTypeId::TIME:
		return make_uniq<AvroValueEncoder<dtime_t, AvroLongOp<dtime_t>>>(allocator, schema);
	case LogicalTypeId::TIMESTAMP:
		return make_uniq<AvroValueEncoder<timestamp_t, AvroLongOp<timestamp_t>>>(allocator, schema);
	case LogicalTypeId::TIMESTAMP_TZ:
		return make_uniq<AvroValueEncoder<timestamp_tz_t, AvroLongOp<timestamp_tz_t>>>(allocator, schema);
	case LogicalTypeId::TIMESTAMP_NS:
		return make_uniq<AvroValueEncoder<timestamp_ns_t, AvroLongOp<timestamp_ns_t>>>(allocator, schema);
	case LogicalTypeId::TIMESTAMP_MS:
		return make_uniq<AvroValueEncoder<timestamp_ms_t, AvroLongOp<timestamp_ms_t>>>(allocator, schema);
	case LogicalTypeId::UUID:
		return make_uniq<AvroValueEncoder<hugeint_t, AvroUUIDOp>>(allocator, schema);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return CreateAvroDecimalEncoder<int16_t>(allocator, schema);
		case PhysicalType::INT32:
			return CreateAvroDecimalEncoder<int32_t>(allocator, schema);
		case PhysicalType::INT64:
			return CreateAvroDecimalEncoder<int64_t>(allocator, schema);
		case PhysicalType::INT128:
			return CreateAvroDecimalEncoder<hugeint_t>(allocator, schema);
		default:
			throw NotImplementedException("Unsupported decimal physical type");
		}
	case LogicalTypeId::SQLNULL:
		return make_uniq<AvroNullEncoder>(allocator, schema);
	case LogicalTypeId::STRUCT:
		return make_uniq<AvroStructEncoder>(allocator, schema);
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return make_uniq<AvroListEncoder>(allocator, schema);
	default:
		throw NotImplementedException("Can't convert logical type '%s' to Avro type", type.ToString());
	}
}

class WriteAvroGlobalState;

class WriteAvroLocalState : public LocalFunctionData {
public:
	WriteAvroLocalState(Allocator &allocator, const WriteAvroBindData &bind_data)
	    : block(allocator), compressor(allocator, bind_data.codec), blocks(allocator) {
		for (auto &column : bind_data.columns) {
			columns.push_back(CreateAvroEncoder(allocator, column));
		}
	}

	void Sink(DataChunk &chunk) {
		const auto count = chunk.size();
		for (idx_t i = 0; i < columns.size(); i++) {
			columns[i]->Encode(chunk.data[i], count);
		}
		ThrowRejected();
		for (idx_t row = 0; row < count; row++) {
			idx_t size = 0;
			for (auto &column : columns) {
				size += column->RowsSize(row, row + 1);
			}
			auto out = block.Alloc(size);
			for (auto &column : columns) {
				out = column->CopyRows(row, row + 1, out);
			}
			block_rows++;
			if (block.GetPosition() >= AVRO_BLOCK_SIZE) {
				FinishBlock();
			}
		}
	}

	void FinishBlock() {
		if (!block_rows) {
			return;
		}
		const auto compressed = compressor.Compress(const_char_ptr_cast(block.GetData()), block.GetPosition());
		auto out = blocks.Alloc(2 * AVRO_MAX_VARINT_SIZE + compressed.size() + AVRO_SYNC_SIZE);
		out += AvroWriteLong(NumericCast<int64_t>(block_rows), out);
		out += AvroWriteLong(NumericCast<int64_t>(compressed.size()), out);
		memcpy(out, compressed.data(), compressed.size());
		out += compressed.size();
		const auto sync = NumericCast<idx_t>(out - blocks.GetData());
		syncs.push_back(sync);
		blocks.SetPosition(sync + AVRO_SYNC_SIZE);
		blocks_rows += block_rows;
		block.Rewind();
		block_rows = 0;
	}

	void Flush(WriteAvroGlobalState &global_state);

private:
	void ThrowRejected() const {
		optional_ptr<const AvroColumnEncoder> column;
		idx_t row = 0;
		for (auto &encoder : columns) {
			if (!encoder->rejected.empty() && (!column || encoder->rejected[0] < row)) {
				column = encoder.get();
				row = encoder->rejected[0];
			}
		}
		if (column) {
			throw InvalidInputException("Cannot insert NULL to non-nullable field of type %s",
			                            LogicalTypeIdToString(column->RejectedType(row).id()));
		}
	}

private:
	vector<unique_ptr<AvroColumnEncoder>> columns;
	MemoryStream block;
	idx_t block_rows = 0;
	AvroCompressor compressor;
	MemoryStream blocks;
	unsafe_vector<idx_t> syncs;
	idx_t blocks_rows = 0;
};

unique_ptr<AsyncFileWriter> OpenAvroFileWriter(ClientContext &context, const string &file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW | FileCompressionType::AUTO_DETECT;
	if (!fs.FileExists(file_path) && !fs.IsPipe(file_path)) {
		flags |= FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
	}
	return make_uniq<AsyncFileWriter>(QueryContext(context), fs, file_path, flags);
}

class WriteAvroGlobalState : public GlobalFunctionData {
public:
	WriteAvroGlobalState(ClientContext &context, const WriteAvroBindData &bind_data, const string &file_path)
	    : file_writer(OpenAvroFileWriter(context, file_path)), compression(file_writer->GetFileCompressionType()) {
		RandomEngine engine;
		engine.RandomData(data_ptr_cast(sync), AVRO_SYNC_SIZE);
		string header;
		AvroWriteHeader(bind_data.metadata, sync, header);
		file_writer->WriteData(const_data_ptr_cast(header.data()), header.size());
	}

	void Write(const_data_ptr_t data, idx_t size, idx_t rows) {
		lock_guard<mutex> guard(lock);
		file_writer->WriteData(data, size);
		row_count += rows;
		file_writer->ApplyBackpressure();
	}

	idx_t FileSize() {
		lock_guard<mutex> guard(lock);
		if (compression == FileCompressionType::UNCOMPRESSED) {
			return file_writer->GetTotalWritten();
		}
		return file_writer->GetPhysicalFileSize();
	}

	void Finalize() {
		file_writer->Close();
		if (written_stats) {
			written_stats->file_size_bytes = file_writer->GetTotalWritten();
			written_stats->row_count = row_count;
		}
	}

	unique_ptr<WriteAvroLocalState> GetLocalState(ClientContext &context, const WriteAvroBindData &bind_data) {
		{
			lock_guard<mutex> guard(local_states_lock);
			if (!local_states.empty()) {
				auto result = std::move(local_states.back());
				local_states.pop_back();
				return result;
			}
		}
		return make_uniq<WriteAvroLocalState>(Allocator::Get(context), bind_data);
	}

	void StoreLocalState(unique_ptr<WriteAvroLocalState> local_state) {
		lock_guard<mutex> guard(local_states_lock);
		local_states.push_back(std::move(local_state));
	}

public:
	char sync[AVRO_SYNC_SIZE];
	optional_ptr<CopyFunctionFileStatistics> written_stats;

private:
	unique_ptr<AsyncFileWriter> file_writer;
	FileCompressionType compression;
	mutex lock;
	idx_t row_count = 0;
	mutex local_states_lock;
	vector<unique_ptr<WriteAvroLocalState>> local_states;
};

void WriteAvroLocalState::Flush(WriteAvroGlobalState &global_state) {
	if (syncs.empty()) {
		return;
	}
	for (auto offset : syncs) {
		memcpy(blocks.GetData() + offset, global_state.sync, AVRO_SYNC_SIZE);
	}
	global_state.Write(blocks.GetData(), blocks.GetPosition(), blocks_rows);
	blocks.Rewind();
	syncs.clear();
	blocks_rows = 0;
}

struct WriteAvroBatchData : public PreparedBatchData {
	WriteAvroBatchData(WriteAvroGlobalState &owner_p, unique_ptr<WriteAvroLocalState> local_state_p)
	    : owner(owner_p), local_state(std::move(local_state_p)) {
	}

	WriteAvroGlobalState &owner;
	unique_ptr<WriteAvroLocalState> local_state;
};

unique_ptr<FunctionData> WriteAvroBind(ClientContext &context, CopyFunctionBindInput &input,
                                       const vector<Identifier> &names, const vector<LogicalType> &sql_types) {
	auto &options = input.info.options;
	identifier_set_t recognized;
	auto user_metadata = ParseMetadata(options, recognized);
	auto column_names = IdentifiersToStrings(names);
	JSONSchemaGenerator generator(column_names, sql_types);
	generator.ParseFieldIds(options, recognized);
	generator.ParseRootName(options, recognized);
	generator.ParseSanitizeFieldNames(options, recognized);
	auto root = generator.Generate();
	auto json_schema = CreateJSONSchema(root);
	auto codec_name = ParseCodec(options, recognized);
	VerifyRecognizedOptions(options, recognized);
	VerifyFieldNames(root);
	auto codec = codec_name.empty() ? AvroCodec::NONE : AvroCodecFromName(codec_name);

	vector<AvroMetadataEntry> metadata;
	metadata.push_back(AvroMetadataEntry {"avro.codec", string(AvroCodecName(codec))});
	metadata.push_back(AvroMetadataEntry {"avro.schema", std::move(json_schema)});
	for (auto &entry : user_metadata) {
		metadata.push_back(std::move(entry));
	}
	return make_uniq<WriteAvroBindData>(std::move(root.children), std::move(metadata), codec);
}

unique_ptr<GlobalFunctionData> WriteAvroInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                         const string &file_path) {
	return make_uniq<WriteAvroGlobalState>(context, bind_data.Cast<WriteAvroBindData>(), file_path);
}

unique_ptr<LocalFunctionData> WriteAvroInitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq<WriteAvroLocalState>(Allocator::Get(context.client), bind_data.Cast<WriteAvroBindData>());
}

void WriteAvroGetWrittenStatistics(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                   CopyFunctionFileStatistics &statistics) {
	gstate.Cast<WriteAvroGlobalState>().written_stats = &statistics;
}

void WriteAvroSink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                   LocalFunctionData &lstate, DataChunk &input) {
	auto &local_state = lstate.Cast<WriteAvroLocalState>();
	local_state.Sink(input);
	local_state.Flush(gstate.Cast<WriteAvroGlobalState>());
}

void WriteAvroCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                      LocalFunctionData &lstate) {
	auto &local_state = lstate.Cast<WriteAvroLocalState>();
	local_state.FinishBlock();
	local_state.Flush(gstate.Cast<WriteAvroGlobalState>());
}

void WriteAvroFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	gstate.Cast<WriteAvroGlobalState>().Finalize();
}

CopyFunctionExecutionMode WriteAvroExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	if (!preserve_insertion_order) {
		return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
	}
	if (supports_batch_index) {
		return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
	}
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

unique_ptr<PreparedBatchData> WriteAvroPrepareBatch(ClientContext &context, FunctionData &bind_data,
                                                    GlobalFunctionData &gstate,
                                                    unique_ptr<ColumnDataCollection> collection) {
	auto &global_state = gstate.Cast<WriteAvroGlobalState>();
	auto local_state = global_state.GetLocalState(context, bind_data.Cast<WriteAvroBindData>());
	for (auto &chunk : collection->Chunks()) {
		local_state->Sink(chunk);
	}
	local_state->FinishBlock();
	return make_uniq<WriteAvroBatchData>(global_state, std::move(local_state));
}

void WriteAvroFlushBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                         PreparedBatchData &batch) {
	auto &avro_batch = batch.Cast<WriteAvroBatchData>();
	avro_batch.local_state->Flush(gstate.Cast<WriteAvroGlobalState>());
	avro_batch.owner.StoreLocalState(std::move(avro_batch.local_state));
}

idx_t WriteAvroFileSizeBytes(GlobalFunctionData &gstate) {
	return gstate.Cast<WriteAvroGlobalState>().FileSize();
}

unique_ptr<FunctionData> ReadAvroCopyBind(ClientContext &context, CopyFromFunctionBindInput &input,
                                          vector<Identifier> &expected_names, vector<LogicalType> &expected_types) {
	auto info = input.info.Copy();
	for (auto option : {"codec", "root_name", "field_ids", "sanitize_field_names", "metadata"}) {
		info->options.erase(option);
	}
	CopyFromFunctionBindInput read_input(*info, input.tf);
	return TableFunctionMultiFileWrapper::MultiFileBindCopyWith(context, read_input, expected_names, expected_types,
	                                                            ReadAvro::GetSingleFileFunction(),
	                                                            ReadAvro::GetMultiFileSettings());
}

} // namespace

CopyFunction AvroCopyFunction::Create() {
	CopyFunction function("avro");
	function.extension = "avro";

	function.copy_from_bind = ReadAvroCopyBind;
	function.copy_from_function = *ReadAvro::GetFunctionSet().functions[0];

	function.copy_to_bind = WriteAvroBind;
	function.copy_to_initialize_local = WriteAvroInitializeLocal;
	function.copy_to_initialize_global = WriteAvroInitializeGlobal;
	function.copy_to_get_written_statistics = WriteAvroGetWrittenStatistics;
	function.copy_to_sink = WriteAvroSink;
	function.copy_to_combine = WriteAvroCombine;
	function.copy_to_finalize = WriteAvroFinalize;
	function.execution_mode = WriteAvroExecutionMode;
	function.prepare_batch = WriteAvroPrepareBatch;
	function.flush_batch = WriteAvroFlushBatch;
	function.file_size_bytes = WriteAvroFileSizeBytes;
	function.parallel_sink = true;
	return function;
}

} // namespace duckdb
