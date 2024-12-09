cmake_minimum_required(VERSION 3.23)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# required by clang tidy
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(AliceDB
        VERSION 0.1
        DESCRIPTION "AliceDB is streaming database"
        LANGUAGES CXX C
        )


add_subdirectory(Source)

#add_subdirectory(third_party)

#option(ENABLE_TESTING "Enable testing" ON)

#if (ENABLE_TESTING)   
#    enable_testing()
#    find_package(GTest REQUIRED)
#    add_subdirectory(tests)
#endif()