add_library(CityHash STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/smhasher/src/City.cpp
)

target_include_directories(CityHash PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/smhasher/src/
)