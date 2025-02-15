#ifndef ALICEDBDISKMANAGER
#define ALICEDBDISKMANAGER

#include "BufferPool.h"
#include "Common.h"

#include <atomic>
#include <filesystem>
#include <future>
#include <liburing.h>
#include <list>
#include <mutex>
#include <thread>

namespace AliceDB {

#define HOLES_THRESHOLD   (0.1)
#define DISK_OP_THRESHOLD 0.01 // (0.01)

enum class DiskOp { Write, Read };

struct Request {
	std::shared_ptr<PageMetadata> page_metadata_;
	DiskOp op_;
};

class DiskManager {
public:
	/**
	 * @brief Open's database file,
	 * spawn worker thread
	 */
	DiskManager(BufferPool *bp, const std::filesystem::path &file);

	/**
	 * @brief ForceFlush all op's
	 * Compact database file
	 * Shut down worker thread
	 */
	~DiskManager();

	
	size_t GetSize() const;

	size_t GetPageCount() const {
		return this->page_count_;
	};

	/**
	 * @brief prepare write page request with given index to the disk,
	 * index must be <= Dabatase file / PageSize
	 * update corresponding buffer pool state to WRITE_WAITING
	 * @return true if page exists on disk false otherwise
	 */
	std::future<bool> WritePage(const index &in_memory_pid);

	/**
	 * @brief reads page with given index to the disk,
	 * index must be <= Dabatase file / PageSize
	 * @return future that is set after completed operation
	 */

	std::future<bool> ReadPage(const index &in_memory_pid);

	/** Remove page from disk
	 *  leaves hole in file
	 *  just append delete to holes list
	 *  and notify to compact if to many holes
	 */
	bool DeletePage(const index &on_disk_page_id);

	/** @brief Resize database file to new_size * PageSize
	 * if new_size > page_count
	 */
	bool Resize(size_t new_size);

	/** @brief Return's free page,
	 * first look's into the list of holes
	 * if it's empty returns new unused page
	 * if there are no pages left, increase disk
	 * file size twice
	 * @return index of the page
	 */
	index AllocatePage();

private:
	// reads and write database meta variables to file
	void ReadMetadata(std::filesystem::path metadata_path);
	void WriteMetadata(std::filesystem::path metadata_path);
	
	/**
	 * @brief Perform all requested operations to disk,
	 * without waiting for DiskManager to do so by itself
	 * Calling thread wait's untill flush is completed
	 * @return bool when operation is completed
	 */
	void ForceFlush();

	/**
	 * @brief Removes holes from on disk file
	 * this operation also modifies Buffer Pool and Table's
	 * updating page's to new indexes so it needs a way to temporary block them
	 *
	 * @note this function is only called by worker_thread
	 * and assumes mutex is being held
	 *
	 * There is another kind of compaction which is Table specific.
	 * Each Table can remove tuples which will create holes in it's
	 * pages.After Table removes enough tuples there will come time when
	 * number of deleted tuples * tuple size > greater than PageSize,
	 * this also needs to be compacted it is however implemented by other
	 * part of the system, here we only care about removing PageSize'd holes
	 */
	int Compact();

	/** submit io_uring  request
	 * remember to update BufferPool metadata
	 * clean free'd page's and add them to free list
	 *
	 * @note this function is only called by worker_thread
	 * and does not need any lock
	 */
	int SubmitDiskRequest();

	/** @brief  worker thread that is waken up when request queue is full ||
	   Disk needs to be compacted or on ForceFlush
	*/
	void WorkerThread();

	const std::filesystem::path file_path_;
	int fd_;
	BufferPool *bp_;

	std::atomic<size_t> page_count_;
	// if max_used_index == page_count
	// allocator need's to resize disk file
	std::atomic<size_t> max_used_index_;
	std::mutex page_mutex_;

	// worker thread
	std::thread worker_;
	std::condition_variable condition_var_;

	/* if true worker thread will set promise to true on succesful disk op*/
	bool set_promise_ = true;

	// stop condition for worker
	bool stop_ = false;

	const unsigned int sleep_duration_ms_ = 5;

	// percentage after which we decide to perform disk operation
	const float disk_op_threshold_ = DISK_OP_THRESHOLD * PAGE_COUNT;
	bool perform_disk_op_;
	// if our worker isn't notified about
	// work that needs to be done it will wake up
	// by itself and perform disk operations
	// read/write requests
	std::mutex work_mutex_;
	std::list<std::unique_ptr<Request>> submit_work_requests_;
	std::list<std::unique_ptr<Request>> perform_work_requests_;
	unsigned int request_count_ = 0;

	// holes created by delete operations
	std::mutex holes_mutex_;
	unsigned int holes_count_ = 0;
	std::list<index> holes_;
	const float holes_threshold_ = HOLES_THRESHOLD;
	bool compact_ = false;

	// io_uring
	struct io_uring ring_;
	struct iovec iov[PAGE_COUNT];
};

} // namespace AliceDB

#endif