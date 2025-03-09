add_library(AliceDB STATIC
    BufferPool.cpp
    DiskManager.cpp
)

find_library(LIBURING_LIBRARY uring REQUIRED)
target_link_libraries(AliceDB PRIVATE ${LIBURING_LIBRARY})
target_link_libraries( AliceDB PRIVATE pthread)
target_link_libraries(AliceDB PUBLIC CityHash)

target_include_directories(AliceDB PUBLIC ${PROJECT_SOURCE_DIR}/Source/Include)
target_include_directories(AliceDB PUBLIC ${PROJECT_SOURCE_DIR}/Source)
