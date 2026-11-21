#include "read_avro.hpp"

#include "avro_reader.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

namespace {

struct ReadSingleAvroFileData : public TableFunctionData {
	OpenFileInfo file;
	AvroSchema schema;
	vector<LogicalType> types;
	bool seekable = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ReadSingleAvroFileData>();
		result->file = file;
		result->schema = schema;
		result->types = types;
		result->seekable = seekable;
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ReadSingleAvroFileData>();
		return file.path == other.file.path && types == other.types;
	}

	shared_ptr<AvroSource> TakeSource() const {
		lock_guard<mutex> guard(source_lock);
		return std::move(source);
	}

	void SetSource(shared_ptr<AvroSource> source_p) {
		lock_guard<mutex> guard(source_lock);
		source = std::move(source_p);
	}

private:
	mutable mutex source_lock;
	mutable shared_ptr<AvroSource> source;
};

struct ReadSingleAvroFileGlobalState : public GlobalTableFunctionState {
	idx_t MaxThreads() const override {
		return global->MaxThreads();
	}

	unique_ptr<AvroReaderGlobal> global;
	vector<column_t> projection_ids;
	vector<LogicalType> scanned_types;
};

struct ReadSingleAvroFileLocalState : public LocalTableFunctionState {
	ReadSingleAvroFileLocalState(ClientContext &context, AvroReaderGlobal &global, const vector<LogicalType> &types)
	    : scan(global) {
		if (!types.empty()) {
			all_columns.Initialize(BufferAllocator::Get(context), types);
		}
	}

	AvroReader scan;
	DataChunk all_columns;
	bool claimed_externally = false;
};

unique_ptr<FunctionData> ReadSingleAvroFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("read_single_avro_file requires a non-NULL file name");
	}
	auto &file_input = TableFunctionFileBindInput::Get(input);
	auto result = make_uniq<ReadSingleAvroFileData>();
	result->file = TableFunctionFileBindInput::GetFile(input);
	shared_ptr<AvroSource> source;
	if (file_input.file_bind_data) {
		source = file_input.file_bind_data->Cast<ReadSingleAvroFileData>().TakeSource();
	}
	if (!source) {
		source = make_shared_ptr<AvroSource>(context, result->file);
	}
	result->schema = ParseAvroSchema(source->header.schema);
	auto root = AvroType::TransformAvroType(result->schema.root_name, result->schema.type);
	vector<MultiFileColumnDefinition> columns;
	if (root.type.id() == LogicalTypeId::STRUCT) {
		columns = std::move(root.children);
	} else {
		columns.push_back(std::move(root));
	}
	for (auto &column : columns) {
		names.push_back(column.name);
		return_types.push_back(column.type);
	}
	if (input.file_info) {
		for (auto &entry : source->header.metadata) {
			input.file_info->metadata.insert(entry.key, Value(entry.value));
		}
		input.file_info->columns = std::move(columns);
	}
	result->types = return_types;
	result->seekable = !source->Stream();
	if (!file_input.schema_only) {
		result->SetSource(std::move(source));
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> ReadSingleAvroFileInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<ReadSingleAvroFileData>();
	auto &file_input = TableFunctionFileInitInput::Get(input);
	auto result = make_uniq<ReadSingleAvroFileGlobalState>();
	const bool projected = input.CanRemoveFilterColumns();
	if (projected) {
		result->projection_ids.assign(input.projection_ids.begin(), input.projection_ids.end());
	}
	AvroReaderColumns columns;
	for (idx_t col_idx = 0; col_idx < input.column_indexes.size(); col_idx++) {
		const auto col_id = input.column_indexes[col_idx].GetPrimaryIndex();
		column_t virtual_id = DConstants::INVALID_INDEX;
		if (IsVirtualColumn(col_id)) {
			virtual_id = col_id;
		} else if (file_input.virtual_columns) {
			auto entry = file_input.virtual_columns->find(col_id);
			if (entry != file_input.virtual_columns->end()) {
				virtual_id = entry->second;
			}
		}
		if (virtual_id == DConstants::INVALID_INDEX) {
			columns.reads.push_back(AvroReaderColumn {col_id, col_idx});
		} else if (virtual_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
			columns.file_row_number = col_idx;
		}
		if (projected) {
			result->scanned_types.push_back(virtual_id == DConstants::INVALID_INDEX ? data.types[col_id]
			                                : virtual_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER
			                                    ? LogicalType::BIGINT
			                                    : LogicalType::BOOLEAN);
		}
	}
	if (input.filters) {
		for (auto &entry : *input.filters) {
			columns.filters.push_back(AvroReaderFilter {entry.GetIndex(), entry.Filter()});
		}
	}
	auto source = data.TakeSource();
	if (!source) {
		source = make_shared_ptr<AvroSource>(context, data.file);
	}
	result->global = make_uniq<AvroReaderGlobal>(context, source, data.schema.type, std::move(columns));
	source->ReadInOrder();
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> ReadSingleAvroFileInitLocal(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<ReadSingleAvroFileGlobalState>();
	return make_uniq<ReadSingleAvroFileLocalState>(context.client, *gstate.global, gstate.scanned_types);
}

bool ReadSingleAvroFileClaimBatch(ClientContext &context, TableFunctionInput &input) {
	auto &lstate = input.local_state->Cast<ReadSingleAvroFileLocalState>();
	lstate.claimed_externally = true;
	return lstate.scan.Claim();
}

void ReadSingleAvroFileFinishBatch(ClientContext &context, TableFunctionInput &input) {
	input.local_state->Cast<ReadSingleAvroFileLocalState>().scan.Release();
}

void ReadSingleAvroFileFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &gstate = input.global_state->Cast<ReadSingleAvroFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleAvroFileLocalState>();
	const bool projected = !gstate.projection_ids.empty();
	auto &target = projected ? lstate.all_columns : output;
	while (true) {
		if (!lstate.scan.Claimed()) {
			if (lstate.claimed_externally || !lstate.scan.Claim()) {
				return;
			}
		}
		if (lstate.scan.Scan(target) != 0) {
			if (projected) {
				output.ReferenceColumns(target, gstate.projection_ids);
			}
			return;
		}
		lstate.scan.Release();
	}
}

double ReadSingleAvroFileProgress(ClientContext &context, const FunctionData *bind_data,
                                  const GlobalTableFunctionState *global_state) {
	if (!global_state) {
		return 0;
	}
	return 100.0 * global_state->Cast<ReadSingleAvroFileGlobalState>().global->Progress();
}

void ReadSingleAvroFileLookup(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &lstate = input.local_state->Cast<ReadSingleAvroFileLocalState>();
	lstate.scan.Lookup(input.pk_lookups, input.pk_survivors, output);
}

bool ReadSingleAvroFileSupportsLookup(optional_ptr<const FunctionData> bind_data) {
	return bind_data && bind_data->Cast<ReadSingleAvroFileData>().seekable;
}

virtual_column_map_t ReadSingleAvroFileVirtualColumns(ClientContext &, optional_ptr<FunctionData>) {
	virtual_column_map_t result;
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	               TableColumn("file_row_number", LogicalType::BIGINT));
	return result;
}

unique_ptr<FunctionData> ReadAvroMultiFileBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	return TableFunctionMultiFileWrapper::MultiFileBindWith(
	    context, input, return_types, names, ReadAvro::GetSingleFileFunction(), ReadAvro::GetMultiFileSettings());
}

} // namespace

TableFunction ReadAvro::GetSingleFileFunction() {
	TableFunction function("read_single_avro_file", FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                       ReadSingleAvroFileFunction, ReadSingleAvroFileBind, ReadSingleAvroFileInitGlobal,
	                       ReadSingleAvroFileInitLocal);
	function.table_scan_progress = ReadSingleAvroFileProgress;
	function.get_virtual_columns = ReadSingleAvroFileVirtualColumns;
	function.projection_pushdown = true;
	function.filter_pushdown = true;
	function.filter_prune = true;
	return function;
}

TableFunctionMultiFileSettings ReadAvro::GetMultiFileSettings() {
	TableFunctionMultiFileSettings settings;
	settings.reader_type = "Avro";
	settings.claim_batch = ReadSingleAvroFileClaimBatch;
	settings.finish_batch = ReadSingleAvroFileFinishBatch;
	settings.lookup = ReadSingleAvroFileLookup;
	settings.supports_lookup = ReadSingleAvroFileSupportsLookup;
	settings.row_storage = true;
	settings.row_id_names = {"file_index", "file_row_number"};
	return settings;
}

unique_ptr<TableRef> ReadAvro::Replacement(ClientContext &context, ReplacementScanInput &input,
                                           optional_ptr<ReplacementScanData> data) {
	auto table_name = ReplacementScan::GetFullPath(input);
	if (!ReplacementScan::CanReplace(table_name, {"avro"})) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(ConstantExpression::String(table_name));
	table_function->function = make_uniq<FunctionExpression>("read_avro", std::move(children));
	if (!FileSystem::HasGlob(table_name)) {
		table_function->alias = Identifier(FileSystem::GetFileSystem(context).ExtractBaseName(table_name));
	}
	return std::move(table_function);
}

TableFunctionSet ReadAvro::GetFunctionSet() {
	auto function = TableFunctionMultiFileWrapper::CreateFunction(GetSingleFileFunction(), "read_avro",
	                                                              GetMultiFileSettings(), ReadAvroMultiFileBind);
	return MultiFileReader::CreateFunctionSet(std::move(function));
}

} // namespace duckdb
