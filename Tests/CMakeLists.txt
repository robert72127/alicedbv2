include(GoogleTest)

file(GLOB TEST_SOURCES "*.cpp")

foreach(TEST_SRC ${TEST_SOURCES})
    get_filename_component(TEST_NAME ${TEST_SRC} NAME_WE)

    add_executable(${TEST_NAME} ${TEST_SRC})

    target_link_libraries(${TEST_NAME} PRIVATE 
        AliceDB 
        GTest::gtest 
        GTest::gtest_main)

        # Add AddressSanitizer compile/link flags
        #target_compile_options(${TEST_NAME} PRIVATE -fsanitize=address -fno-omit-frame-pointer -g -O1)
        #target_link_options(${TEST_NAME} PRIVATE -fsanitize=address)
        
        
        #target_compile_options(${TEST_NAME} PRIVATE -fsanitize=thread -g -O1)
        #target_link_options(${TEST_NAME} PRIVATE -fsanitize=thread)

        # Discover and register tests with CTest
        gtest_discover_tests(${TEST_NAME})
endforeach()