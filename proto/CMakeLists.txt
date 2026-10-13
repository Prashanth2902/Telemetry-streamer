# Generated Protobuf code for the wire schema.
#
# Its own target, without tstream_set_warnings (ADR-008): -Werror covers only
# code we write. The generated headers are SYSTEM includes so their warnings
# don't surface in our translation units either.
add_library(tstream_proto STATIC telemetry.proto)
protobuf_generate(TARGET tstream_proto LANGUAGE cpp)
target_include_directories(tstream_proto SYSTEM PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(tstream_proto PUBLIC protobuf::libprotobuf)
