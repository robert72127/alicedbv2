include(GoogleTest)

file(GLOB TEST_SOURCES "*.cpp")

foreach(TEST_SRC ${TEST_SOURCES})
    get_filename_component(TEST_NAME ${TEST_SRC} NAME_WE)

    add_executable(${TEST_NAME} ${TEST_SRC})

    target_link_libraries(${TEST_NAME} PRIVATE 
        AliceDB 
        GTest::gtest 
        GTest::gtest_main)

        # Discover and register tests with CTest
        gtest_discover_tests(${TEST_NAME})
endforeach()