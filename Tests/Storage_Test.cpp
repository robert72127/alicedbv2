#include "Storage/BufferPool.h"
#include "Storage/DiskManager.h"
#include "Common.h"

#include "gtest/gtest.h"

#include <filesystem>
#include <thread>
#include <vector>
#include <filesystem>

TEST(BufferPoolTest, AllocationTest) {
    auto *bp = new AliceDB::BufferPool();
    auto *dm = new AliceDB::DiskManager(bp, "file1.db");
    ASSERT_EQ(true, bp->SetDisk(dm));

    std::vector<AliceDB::index> pids;
    for (int i = 0; i < PAGE_COUNT; i++) {
        AliceDB::index idx = bp->CreatePage();
        pids.push_back(idx);
    }

    for (int i = 0; i < PAGE_COUNT; i ++) {
            bp->UnpinPage(pids[i]);
    }
    
    
    pids.clear();

    for (int j = 0; j < 10; j++) {

        for (int i = 0; i < PAGE_COUNT; i ++ ) {
            AliceDB::index idx = bp->CreatePage();
            pids.push_back(idx);
        }

        for(int i = 0; i <  PAGE_COUNT; i++){
            bp->UnpinPage(pids[i]);
        }
    }

    // Cleanup
    delete dm;
    delete bp;

    // Delete database file
    std::filesystem::remove("file1.db");
}

TEST(BufferPoolTest, ReadWriteTest){

    // create disk manager & buffer pool
    auto *bp = new AliceDB::BufferPool(); 
    auto *dm = new AliceDB::DiskManager(bp, "file2.db");

    ASSERT_EQ(true, bp->SetDisk(dm));

    std::vector<AliceDB::index>disk_pids;
    for(int i = 0; i < PAGE_COUNT; i++){
        
        AliceDB::index idx = bp->CreatePage();   
        disk_pids.push_back(bp->GetDiskIndex(idx));

        bp->UnpinPage(idx);
    }

    std::vector<AliceDB::index> mem_indexes;
    for(int i = 0; i < PAGE_COUNT; i++){
        AliceDB::index memdex;
        ASSERT_EQ(true, bp->GetPageReadonly(&memdex,disk_pids[i]));

        mem_indexes.push_back(memdex);
        const char* data = bp->GetDataReadonly(memdex);
    }


    for(int i = 0; i < PAGE_COUNT; i++){
        bp->UnpinPage(mem_indexes[i]);
    }


    for(int i = 0; i < PAGE_COUNT; i++){
        AliceDB::index memdex;
        ASSERT_EQ(true, bp->GetPageWriteable(&memdex,disk_pids[i]));

        char* data = bp->GetDataWriteable(memdex);
        
        //*data = 1;
        for(int j = 0; j< PageSize; j++){
            *(data+j) = static_cast<char>( i % 10);
        }

        bp->UnpinPage(memdex);
    }

    for(int i = 0; i < PAGE_COUNT; i++){
        AliceDB::index memdex;
        ASSERT_EQ(true, bp->GetPageReadonly(&memdex,disk_pids[i]));

        const char* data = bp->GetDataReadonly(memdex);
       
        for(int j = 0; j< PageSize; j++){
            ASSERT_EQ(*(data+j), static_cast<char>( i % 10) );
        }

        bp->UnpinPage(memdex);
    }


    // Cleanup
    delete dm;
    delete bp;
    
    // delete database file
    std::filesystem::remove("file2.db");
}

#define THREAD_COUNT 8

TEST(BufferPoolTest, MultithreadedReadWriteTest1) {
    // Create disk manager & buffer pool
    auto *bp = new AliceDB::BufferPool;
    auto *dm = new AliceDB::DiskManager(bp, "file3.db");

    ASSERT_EQ(true, bp->SetDisk(dm));

    // Create pages and get their disk indexes
    std::vector<AliceDB::index> disk_pids;
    for (int i = 0; i < PAGE_COUNT; i++) {
        AliceDB::index idx = bp->CreatePage();
        disk_pids.push_back(bp->GetDiskIndex(idx));
        bp->UnpinPage(idx);
    }


    // Function for each thread to perform read and write operations
    auto thread_func = [&](int thread_id, int start_idx, int end_idx) {
        std::vector<AliceDB::index> mem_indexes;

        // Read pages
        for (int i = start_idx; i < end_idx; i++) {
            AliceDB::index memdex;
            
            ASSERT_EQ(true, bp->GetPageReadonly(&memdex, disk_pids[i]));
            mem_indexes.push_back(memdex);

            const char *data = bp->GetDataReadonly(memdex);
            // Perform any necessary read operations
            bp->UnpinPage(memdex);
        }

        // Write to pages
        for (int i = start_idx; i < end_idx; i++) {
            AliceDB::index memdex;
            ASSERT_EQ(true, bp->GetPageWriteable(&memdex, disk_pids[i]));

            char *data = bp->GetDataWriteable(memdex);
            // Write data specific to this thread
            for (int j = 0; j < PageSize; j++) {
                data[j] = static_cast<char>((thread_id + i + j) % 256);
            }
            bp->UnpinPage(memdex);
        }

        // Verify written data
        for (int i = start_idx; i < end_idx; i++) {
            AliceDB::index memdex;
            ASSERT_EQ(true, bp->GetPageReadonly(&memdex, disk_pids[i]));

            const char *data = bp->GetDataReadonly(memdex);
            for (int j = 0; j < PageSize; j++) {
                ASSERT_EQ(data[j], static_cast<char>((thread_id + i + j) % 256));
            }
            bp->UnpinPage(memdex);
        }
    };

    // Create threads
    std::vector<std::thread> threads;
    int pages_per_thread = PAGE_COUNT / THREAD_COUNT;
    for (int t = 0; t < THREAD_COUNT; t++) {
        int start_idx = t * pages_per_thread;
        int end_idx = (t == THREAD_COUNT - 1) ? PAGE_COUNT : start_idx + pages_per_thread;
        threads.emplace_back(thread_func, t, start_idx, end_idx);
    }


    // Join threads
    for (auto &th : threads) {
        if (th.joinable()) {
            th.join();
        }
    }

    // Clean up
   delete dm;
   delete bp;

    // Delete database file
    std::filesystem::remove("file3.db");
}

TEST(BufferPoolTest, MultithreadedCreateReadWriteTest2) {
    // Create disk manager & buffer pool
    auto *bp = new AliceDB::BufferPool;
    auto *dm = new AliceDB::DiskManager(bp, "file4.db");

    ASSERT_EQ(true, bp->SetDisk(dm));

    // Mutex for synchronizing access to shared resources
    std::mutex disk_pids_mutex;

    // Shared vector to store disk page IDs
    std::vector<AliceDB::index> disk_pids;

    // Function for each thread to create pages and perform read and write operations
    auto thread_func = [&](int thread_id, int start_idx, int end_idx) {
        std::vector<AliceDB::index> local_disk_pids;
        std::vector<AliceDB::index> mem_indexes;

        // Create pages
        for (int i = start_idx; i < end_idx; i++) {
            AliceDB::index idx = bp->CreatePage();

            // Store disk index
            AliceDB::index disk_idx = bp->GetDiskIndex(idx);

            {
                // Lock mutex before modifying the shared disk_pids vector
                std::lock_guard<std::mutex> lock(disk_pids_mutex);
                disk_pids.push_back(disk_idx);
            }

            bp->UnpinPage(idx);
        }

        // Read pages
        for (int i = start_idx; i < end_idx; i++) {
            AliceDB::index disk_idx;

            {
                // Lock mutex to safely access the shared disk_pids vector
                std::lock_guard<std::mutex> lock(disk_pids_mutex);
                disk_idx = disk_pids[i];
            }

            AliceDB::index memdex;
            ASSERT_EQ(true, bp->GetPageReadonly(&memdex, disk_idx));
            mem_indexes.push_back(memdex);

            const char *data = bp->GetDataReadonly(memdex);
            // Perform any necessary read operations
            bp->UnpinPage(memdex);
        }

        // Write to pages
        for (int i = start_idx; i < end_idx; i++) {
            AliceDB::index disk_idx;

            {
                // Lock mutex to safely access the shared disk_pids vector
                std::lock_guard<std::mutex> lock(disk_pids_mutex);
                disk_idx = disk_pids[i];
            }

            AliceDB::index memdex;
            ASSERT_EQ(true, bp->GetPageWriteable(&memdex, disk_idx));

            char *data = bp->GetDataWriteable(memdex);
            // Write data specific to this thread
            for (int j = 0; j < PageSize; j++) {
                data[j] = static_cast<char>((thread_id + i + j) % 256);
            }
            bp->UnpinPage(memdex);
        }

        // Verify written data
        for (int i = start_idx; i < end_idx; i++) {
            AliceDB::index disk_idx;

            {
                // Lock mutex to safely access the shared disk_pids vector
                std::lock_guard<std::mutex> lock(disk_pids_mutex);
                disk_idx = disk_pids[i];
            }

            AliceDB::index memdex;
            ASSERT_EQ(true, bp->GetPageReadonly(&memdex, disk_idx));

            const char *data = bp->GetDataReadonly(memdex);
            for (int j = 0; j < PageSize; j++) {
                ASSERT_EQ(data[j], static_cast<char>((thread_id + i + j) % 256));
            }
            bp->UnpinPage(memdex);
        }
    };

    // Calculate pages per thread
    int pages_per_thread = PAGE_COUNT / THREAD_COUNT;
    int extra_pages = PAGE_COUNT % THREAD_COUNT;

    // Create threads
    std::vector<std::thread> threads;
    int current_idx = 0;
    for (int t = 0; t < THREAD_COUNT; t++) {
        int start_idx = current_idx;
        int end_idx = start_idx + pages_per_thread + (t < extra_pages ? 1 : 0);
        current_idx = end_idx;
        threads.emplace_back(thread_func, t, start_idx, end_idx);
    }

    // Join threads
    for (auto &th : threads) {
        if (th.joinable()) {
            th.join();
        }
    }


    // Clean up
    delete dm;
    delete bp;

    // Delete database file
    std::filesystem::remove("file4.db");
}