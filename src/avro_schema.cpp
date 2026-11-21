#include "avro_type.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"

namespace duckdb {

namespace {

constexpr int64_t MAX_DECIMAL_WIDTH = 38;

[[noreturn]] void ThrowSchema(const string &message) {
	throw InvalidInputException("Invalid Avro schema: %s", message);
}

std::string_view StringMember(const JSONNode *object, std::string_view key) {
	if (!object) {
		return {};
	}
	auto member = object->Get(key);
	if (!member || member->type != JSONValueType::STRING) {
		return {};
	}
	return member->GetString();
}

bool IntegerMember(const JSONNode &object, std::string_view key, int64_t &value) {
	auto member = object.Get(key);
	if (!member) {
		return false;
	}
	if (member->type == JSONValueType::SIGNED_INTEGER) {
		value = member->signed_integer;
		return true;
	}
	if (member->type == JSONValueType::UNSIGNED_INTEGER &&
	    member->unsigned_integer <= static_cast<uint64_t>(NumericLimits<int64_t>::Maximum())) {
		value = static_cast<int64_t>(member->unsigned_integer);
		return true;
	}
	return false;
}

void SetFieldId(const JSONNode &object, std::string_view key, AvroType &type) {
	int64_t id;
	if (IntegerMember(object, key, id) && id >= NumericLimits<int32_t>::Minimum() &&
	    id < NumericLimits<int32_t>::Maximum()) {
		type.field_id = static_cast<int32_t>(id);
	}
}

AvroType Temporal(AvroKind kind, LogicalType type, AvroTimeUnit unit) {
	AvroType result(kind, std::move(type));
	result.unit = unit;
	return result;
}

bool MakeDecimal(const JSONNode &object, LogicalType &type) {
	int64_t precision;
	int64_t scale = 0;
	if (!IntegerMember(object, "precision", precision)) {
		return false;
	}
	if (object.Get("scale") && !IntegerMember(object, "scale", scale)) {
		return false;
	}
	if (precision < 1 || scale < 0 || scale > precision) {
		return false;
	}
	if (precision > MAX_DECIMAL_WIDTH) {
		throw NotImplementedException("Avro decimal precision %lld is larger than the supported maximum of %lld",
		                              precision, MAX_DECIMAL_WIDTH);
	}
	type = LogicalType::DECIMAL(static_cast<uint8_t>(precision), static_cast<uint8_t>(scale));
	return true;
}

struct NamedDefinition {
	reference<const JSONNode> node;
	string space;
};

class SchemaParser {
public:
	AvroType Parse(const JSONNode &node, const string &space);

private:
	AvroType Primitive(std::string_view type, const JSONNode *object);
	AvroType Named(const JSONNode &object, std::string_view type, const string &space);
	AvroType Record(const JSONNode &object, const string &name, const string &full_name, const string &record_space);
	AvroType Enum(const JSONNode &object);
	AvroType Fixed(const JSONNode &object);
	AvroType Array(const JSONNode &object, const string &space);
	AvroType Map(const JSONNode &object, const string &space);
	AvroType Union(const JSONNode &array, const string &space);
	AvroType Reference(std::string_view name, const string &space);

private:
	unordered_map<string, NamedDefinition> named;
	unordered_set<string> expanding;
};

AvroType SchemaParser::Parse(const JSONNode &node, const string &space) {
	if (node.type == JSONValueType::ARRAY) {
		return Union(node, space);
	}
	const JSONNode *object = nullptr;
	std::string_view type;
	if (node.type == JSONValueType::STRING) {
		type = node.GetString();
	} else if (node.type == JSONValueType::OBJECT) {
		object = &node;
		auto type_node = node.Get("type");
		if (!type_node || type_node->type != JSONValueType::STRING) {
			ThrowSchema("\"type\" field must be a string");
		}
		type = type_node->GetString();
	} else {
		ThrowSchema("a type must be a string, an object or an array");
	}
	if (type == "record" || type == "enum" || type == "fixed") {
		if (!object) {
			ThrowSchema(StringUtil::Format("a %s type must be an object", string(type)));
		}
		return Named(*object, type, space);
	}
	if (type == "array" || type == "map") {
		if (!object) {
			ThrowSchema(StringUtil::Format("an %s type must be an object", string(type)));
		}
		return type == "array" ? Array(*object, space) : Map(*object, space);
	}
	auto primitive = Primitive(type, object);
	if (primitive.duckdb_type.id() != LogicalTypeId::INVALID) {
		return primitive;
	}
	return Reference(type, space);
}

AvroType SchemaParser::Primitive(std::string_view type, const JSONNode *object) {
	const auto logical = StringMember(object, "logicalType");
	if (type == "null") {
		return AvroType(AvroKind::NULL_TYPE, LogicalType::SQLNULL);
	}
	if (type == "boolean") {
		return AvroType(AvroKind::BOOLEAN, LogicalType::BOOLEAN);
	}
	if (type == "int") {
		if (logical == "date") {
			return AvroType(AvroKind::INT, LogicalType::DATE);
		}
		if (logical == "time-millis") {
			return Temporal(AvroKind::INT, LogicalType::TIME, AvroTimeUnit::MILLIS);
		}
		return AvroType(AvroKind::INT, LogicalType::INTEGER);
	}
	if (type == "long") {
		auto adjust = object ? object->Get("adjust-to-utc") : nullptr;
		const bool utc = adjust && adjust->type == JSONValueType::BOOLEAN && adjust->boolean;
		const auto instant = utc ? LogicalType::TIMESTAMP_TZ : LogicalType::TIMESTAMP;
		if (logical == "time-micros") {
			return Temporal(AvroKind::LONG, LogicalType::TIME, AvroTimeUnit::MICROS);
		}
		if (logical == "timestamp-millis") {
			return Temporal(AvroKind::LONG, instant, AvroTimeUnit::MILLIS);
		}
		if (logical == "timestamp-micros") {
			return Temporal(AvroKind::LONG, instant, AvroTimeUnit::MICROS);
		}
		if (logical == "timestamp-nanos") {
			if (utc) {
				throw NotImplementedException("Avro timestamp-nanos with adjust_to_utc not supported");
			}
			return Temporal(AvroKind::LONG, LogicalType::TIMESTAMP_NS, AvroTimeUnit::NANOS);
		}
		if (logical == "local-timestamp-millis") {
			return Temporal(AvroKind::LONG, LogicalType::TIMESTAMP, AvroTimeUnit::MILLIS);
		}
		if (logical == "local-timestamp-micros") {
			return Temporal(AvroKind::LONG, LogicalType::TIMESTAMP, AvroTimeUnit::MICROS);
		}
		if (logical == "local-timestamp-nanos") {
			return Temporal(AvroKind::LONG, LogicalType::TIMESTAMP_NS, AvroTimeUnit::NANOS);
		}
		return AvroType(AvroKind::LONG, LogicalType::BIGINT);
	}
	if (type == "float") {
		return AvroType(AvroKind::FLOAT, LogicalType::FLOAT);
	}
	if (type == "double") {
		return AvroType(AvroKind::DOUBLE, LogicalType::DOUBLE);
	}
	if (type == "bytes") {
		LogicalType decimal;
		if (logical == "decimal" && MakeDecimal(*object, decimal)) {
			return AvroType(AvroKind::BYTES, std::move(decimal));
		}
		return AvroType(AvroKind::BYTES, LogicalType::BLOB);
	}
	if (type == "string") {
		if (logical == "uuid") {
			return AvroType(AvroKind::STRING, LogicalType::UUID);
		}
		return AvroType(AvroKind::STRING, LogicalType::VARCHAR);
	}
	return AvroType();
}

AvroType SchemaParser::Named(const JSONNode &object, std::string_view type, const string &space) {
	auto name_node = object.Get("name");
	if (!name_node || name_node->type != JSONValueType::STRING) {
		ThrowSchema(StringUtil::Format("%s type must have a \"name\"", string(type)));
	}
	string name(name_node->GetString());
	string name_space;
	const auto dot = name.rfind('.');
	if (dot != string::npos) {
		name_space = name.substr(0, dot);
		name = name.substr(dot + 1);
	} else if (auto ns = object.Get("namespace"); ns && ns->type == JSONValueType::STRING) {
		name_space = string(ns->GetString());
	} else {
		name_space = space;
	}
	const string full_name = name_space.empty() ? name : name_space + "." + name;
	named.insert_or_assign(full_name, NamedDefinition {object, space});
	if (type == "record") {
		return Record(object, name, full_name, name_space);
	}
	return type == "enum" ? Enum(object) : Fixed(object);
}

AvroType SchemaParser::Record(const JSONNode &object, const string &name, const string &full_name,
                              const string &record_space) {
	auto fields = object.Get("fields");
	if (!fields || fields->type != JSONValueType::ARRAY) {
		ThrowSchema("record type must have \"fields\"");
	}
	if (!expanding.insert(full_name).second) {
		throw InvalidInputException("Recursive Avro types not supported: %s", name);
	}
	child_list_t<AvroType> children;
	for (auto &field : fields->Elements()) {
		if (field.type != JSONValueType::OBJECT) {
			ThrowSchema(StringUtil::Format("field %llu of record %s must be an object", children.size(), name));
		}
		auto field_name = field.Get("name");
		auto field_type = field.Get("type");
		if (!field_name || field_name->type != JSONValueType::STRING) {
			ThrowSchema(StringUtil::Format("field %llu of record %s must have a \"name\"", children.size(), name));
		}
		if (!field_type) {
			ThrowSchema(StringUtil::Format("field %llu of record %s must have a \"type\"", children.size(), name));
		}
		if (field_name->size == 0) {
			throw InvalidInputException("Empty avro field name");
		}
		auto child = Parse(*field_type, record_space);
		SetFieldId(field, "field-id", child);
		children.emplace_back(string(field_name->GetString()), std::move(child));
	}
	expanding.erase(full_name);
	if (children.empty()) {
		return AvroType(AvroKind::RECORD, LogicalType::SQLNULL);
	}
	return AvroType(AvroKind::RECORD, LogicalTypeId::STRUCT, std::move(children));
}

AvroType SchemaParser::Enum(const JSONNode &object) {
	auto symbols = object.Get("symbols");
	if (!symbols || symbols->type != JSONValueType::ARRAY || symbols->size == 0) {
		ThrowSchema("enum type must have a non-empty \"symbols\" array");
	}
	const idx_t size = symbols->size;
	Vector levels(LogicalType::VARCHAR, size);
	auto levels_data = FlatVector::GetDataMutable<string_t>(levels);
	idx_t index = 0;
	for (auto &symbol : symbols->Elements()) {
		if (symbol.type != JSONValueType::STRING) {
			ThrowSchema("enum symbols must be strings");
		}
		levels_data[index++] = StringVector::AddString(levels, symbol.GetString().data(), symbol.size);
	}
	return AvroType(AvroKind::ENUM, LogicalType::ENUM(levels, size));
}

AvroType SchemaParser::Fixed(const JSONNode &object) {
	int64_t size;
	if (!IntegerMember(object, "size", size) || size < 0) {
		ThrowSchema("fixed type must have a non-negative integer \"size\"");
	}
	const auto logical = StringMember(&object, "logicalType");
	LogicalType type = LogicalType::BLOB;
	if (logical == "decimal") {
		MakeDecimal(object, type);
	} else if (logical == "uuid" && size == 16) {
		type = LogicalType::UUID;
	} else if (logical == "duration" && size == 12) {
		type = LogicalType::INTERVAL;
	}
	AvroType result(AvroKind::FIXED, std::move(type));
	result.fixed_size = static_cast<idx_t>(size);
	return result;
}

AvroType SchemaParser::Array(const JSONNode &object, const string &space) {
	auto items = object.Get("items");
	if (!items) {
		ThrowSchema("array type must have \"items\"");
	}
	auto item = Parse(*items, space);
	SetFieldId(object, "element-id", item);
	bool map = StringMember(&object, "logicalType") == "map";
	if (map) {
		const AvroType *entry = &item;
		if (entry->kind == AvroKind::UNION && entry->union_child_map.size() == 1) {
			for (auto &branch : entry->children) {
				if (branch.second.duckdb_type.id() != LogicalTypeId::SQLNULL) {
					entry = &branch.second;
				}
			}
		}
		map = entry->kind == AvroKind::RECORD && entry->children.size() == 2;
	}
	child_list_t<AvroType> children;
	children.emplace_back("list_entry", std::move(item));
	return AvroType(AvroKind::ARRAY, map ? LogicalTypeId::MAP : LogicalTypeId::LIST, std::move(children));
}

AvroType SchemaParser::Map(const JSONNode &object, const string &space) {
	auto values = object.Get("values");
	if (!values) {
		ThrowSchema("map type must have \"values\"");
	}
	AvroType key(AvroKind::STRING, LogicalType::VARCHAR);
	SetFieldId(object, "key-id", key);
	auto value = Parse(*values, space);
	SetFieldId(object, "value-id", value);
	child_list_t<AvroType> children;
	children.emplace_back("key_entry", std::move(key));
	children.emplace_back("value_entry", std::move(value));
	return AvroType(AvroKind::MAP, LogicalTypeId::MAP, std::move(children));
}

AvroType SchemaParser::Union(const JSONNode &array, const string &space) {
	if (array.size == 0) {
		ThrowSchema("union type must have at least one branch");
	}
	child_list_t<AvroType> children;
	unordered_map<idx_t, optional_idx> members;
	idx_t member = 0;
	for (auto &branch : array.Elements()) {
		auto type = Parse(branch, space);
		const idx_t index = children.size();
		if (type.duckdb_type.id() != LogicalTypeId::SQLNULL) {
			members[index] = member++;
		}
		children.emplace_back(StringUtil::Format("u%llu", index), std::move(type));
	}
	return AvroType(AvroKind::UNION, LogicalTypeId::UNION, std::move(children), std::move(members));
}

AvroType SchemaParser::Reference(std::string_view name, const string &space) {
	string full_name(name);
	if (!space.empty() && name.find('.') == std::string_view::npos) {
		full_name = space + "." + full_name;
	}
	auto entry = named.find(full_name);
	if (entry == named.end()) {
		entry = named.find(name);
	}
	if (entry == named.end()) {
		ThrowSchema(StringUtil::Format("unknown type \"%s\"", string(name)));
	}
	const auto definition = entry->second;
	return Parse(definition.node.get(), definition.space);
}

} // namespace

AvroSchema ParseAvroSchema(const string &json) {
	JSONTreeParser parser;
	JSONParseError error;
	auto root = parser.Parse(json.data(), json.size(), error);
	if (!root) {
		throw InvalidInputException("Avro schema is not valid JSON: %s", error.message);
	}
	AvroSchema result;
	SchemaParser schema_parser;
	result.type = schema_parser.Parse(*root, string());
	result.root_name = "avro_schema";
	if (root->type == JSONValueType::OBJECT) {
		const auto type = StringMember(root, "type");
		const auto name = StringMember(root, "name");
		if ((type == "record" || type == "enum" || type == "fixed") && !name.empty()) {
			const auto dot = name.rfind('.');
			result.root_name = string(dot == std::string_view::npos ? name : name.substr(dot + 1));
		}
	}
	return result;
}

} // namespace duckdb
