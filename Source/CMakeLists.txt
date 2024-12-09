#add_library(AliceDB STATIC )
add_executable(AliceDB main.cpp)
TARGET_LINK_LIBRARIES( AliceDB PRIVATE pthread)

target_include_directories(AliceDB PRIVATE  ${PROJECT_SOURCE_DIR}/Source/Include)

# link other libs
# target_link_libraries(AliceDB PRIVATE)