cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME avro)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/avro_binary.cpp
    src/avro_copy.cpp
    src/avro_extension.cpp
    src/avro_metadata.cpp
    src/avro_reader.cpp
    src/avro_schema.cpp
    src/field_ids.cpp
    src/read_avro.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} snappy)
target_link_libraries(${LOADABLE_EXTENSION_NAME} snappy)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
