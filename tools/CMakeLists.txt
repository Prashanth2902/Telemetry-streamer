# tstream-recv: the receiving side (SPEC §3).
add_executable(tstream-recv receiver/main.cpp)
target_link_libraries(tstream-recv PRIVATE tstream::core)
tstream_set_warnings(tstream-recv)
