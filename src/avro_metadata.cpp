#include "avro_metadata.hpp"

#include "avro_reader.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

namespace {

struct AvroMetadataBindData : public TableFunctionData {
	string file_path;
};

struct AvroMetadataGlobalState : public GlobalTableFunctionState {
	vector<AvroMetadataEntry> metadata;
	idx_t offset = 0;
};

unique_ptr<FunctionData> AvroMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<AvroMetadataBindData>();
	result->file_path = input.inputs[0].ToString();
	names.emplace_back("key");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> AvroMetadataInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<AvroMetadataBindData>();
	auto result = make_uniq<AvroMetadataGlobalState>();
	AvroSource source(context, OpenFileInfo(bind_data.file_path));
	result->metadata = std::move(source.header.metadata);
	for (auto &entry : result->metadata) {
		if (!Utf8Proc::IsValid(entry.key.c_str(), entry.key.size())) {
			throw InvalidInputException("Avro file \"%s\" has a metadata key that is not valid UTF-8",
			                            bind_data.file_path);
		}
		if (!Utf8Proc::IsValid(entry.value.c_str(), entry.value.size())) {
			throw InvalidInputException("Avro file \"%s\" has a metadata value for \"%s\" that is not valid UTF-8",
			                            bind_data.file_path, entry.key);
		}
	}
	return std::move(result);
}

void AvroMetadataFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<AvroMetadataGlobalState>();
	const idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, gstate.metadata.size() - gstate.offset);
	auto keys = FlatVector::Writer<string_t>(output.data[0], count);
	auto values = FlatVector::Writer<string_t>(output.data[1], count);
	for (idx_t i = 0; i < count; i++) {
		auto &entry = gstate.metadata[gstate.offset + i];
		keys.WriteValue(string_t(entry.key));
		values.WriteValue(string_t(entry.value));
	}
	gstate.offset += count;
	output.SetChildCardinality(count);
}

} // namespace

TableFunction AvroMetadata::GetFunction() {
	return TableFunction("avro_metadata", FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                     AvroMetadataFunction, AvroMetadataBind, AvroMetadataInit);
}

} // namespace duckdb
