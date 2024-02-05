cmake_minimum_required(VERSION 3.4)

add_executable(testhdfs testhdfs.cc)
target_link_libraries(testhdfs ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
