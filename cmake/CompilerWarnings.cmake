# Project warning flags, applied per target with tstream_set_warnings(<target>).
# Kept off third-party code so -Werror only covers what we write.

function(tstream_set_warnings target)
  set(flags
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow
    -Wconversion
    -Wsign-conversion
    -Wold-style-cast
    -Wnon-virtual-dtor
    -Woverloaded-virtual
    -Wnull-dereference
    -Wdouble-promotion
    -Wimplicit-fallthrough)

  if(TSTREAM_WARNINGS_AS_ERRORS)
    list(APPEND flags -Werror)
  endif()

  target_compile_options(${target} PRIVATE ${flags})
endfunction()
