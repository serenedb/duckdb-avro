cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME avro)

set(AVRO_INCLUDE_DIR "")
set(ALL_AVRO_LIBRARIES "avro-static")

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/avro_extension.cpp src/avro_reader.cpp src/avro_copy.cpp
    src/field_ids.cpp src/avro_multi_file_info.cpp src/avro_metadata.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_include_directories(${EXTENSION_NAME} PRIVATE ${AVRO_INCLUDE_DIR})
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${AVRO_INCLUDE_DIR})

# Link against avro-static - use plain signature to match DuckDB's build system
target_link_libraries(${EXTENSION_NAME} avro-static)
target_link_libraries(${LOADABLE_EXTENSION_NAME} avro-static)

# Mark avro-static as part of the export set
install(
  TARGETS ${EXTENSION_NAME} avro-static
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
