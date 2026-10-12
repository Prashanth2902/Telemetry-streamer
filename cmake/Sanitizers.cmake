# Global sanitizer flags, selected with -DTSTREAM_SANITIZER=none|address|thread.
# Applied to every target so instrumentation is consistent across the build.

if(TSTREAM_SANITIZER STREQUAL "address")
  # ASan + UBSan. UBSan findings abort instead of just printing.
  set(_tstream_san_flags
    -fsanitize=address,undefined
    -fno-sanitize-recover=undefined
    -fno-omit-frame-pointer)
elseif(TSTREAM_SANITIZER STREQUAL "thread")
  set(_tstream_san_flags
    -fsanitize=thread
    -fno-omit-frame-pointer)
elseif(TSTREAM_SANITIZER STREQUAL "none")
  set(_tstream_san_flags "")
else()
  message(FATAL_ERROR
    "TSTREAM_SANITIZER must be none, address, or thread (got '${TSTREAM_SANITIZER}')")
endif()

if(_tstream_san_flags)
  message(STATUS "tstream: sanitizer = ${TSTREAM_SANITIZER}")
  add_compile_options(${_tstream_san_flags})
  add_link_options(${_tstream_san_flags})
endif()
