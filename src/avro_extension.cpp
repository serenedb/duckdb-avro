#include "avro_extension.hpp"

#include "avro_copy.hpp"
#include "avro_metadata.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "read_avro.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ReadAvro::GetFunctionSet());
	loader.RegisterFunction(AvroMetadata::GetFunction());
	loader.RegisterFunction(AvroCopyFunction::Create());
	DBConfig::GetConfig(loader.GetDatabaseInstance()).replacement_scans.emplace_back(ReadAvro::Replacement);
}

void AvroExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string AvroExtension::Name() {
	return "avro";
}

std::string AvroExtension::Version() const {
#ifdef EXT_VERSION_AVRO
	return EXT_VERSION_AVRO;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(avro, loader) {
	duckdb::LoadInternal(loader);
}
}
