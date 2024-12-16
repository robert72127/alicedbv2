add_library(AliceDB INTERFACE)

# TARGET_LINK_LIBRARIES( AliceDB PRIVATE pthread)

target_include_directories(AliceDB INTERFACE ${PROJECT_SOURCE_DIR}/Source/Include)

# link other libs
#target_link_libraries(AliceDB PRIVATE)