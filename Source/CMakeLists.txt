add_library(AliceDB STATIC
    Storage/BufferPool.cpp
    Storage/DiskManager.cpp
)

find_library(LIBURING_LIBRARY uring REQUIRED)
target_link_libraries(AliceDB PRIVATE ${LIBURING_LIBRARY})
TARGET_LINK_LIBRARIES( AliceDB PRIVATE pthread)


target_include_directories(AliceDB PUBLIC ${PROJECT_SOURCE_DIR}/Source/Include)

# link other libs
#target_link_libraries(AliceDB PRIVATE)