#ifndef ALICEDBBUFFERPOOL
#define ALICEDBBUFFERPOOL

#include "Common.h"

#include <array>
#include <cstring>
#include <future>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace AliceDB {

struct IndexStorage {

	bool contains(const index &item) {
		return (map_.count(item) != 0);
	}

	bool erase(const index &item) {
		auto it = map_.find(item);
		if (it == map_.end()) {
			return false; // Item not found.
		}
		// Erase from list and map
		list_.erase(it->second);
		map_.erase(it);
		return true;
	}

	// insert index at the end of structure
	void insert(const index &item) {
		// Check if the item already exists (optional, based on requirements).
		if (map_.find(item) != map_.end()) {
			// Item already exists; handle as needed (e.g., ignore or update).
			return;
		}
		list_.emplace_back(item);
		// Get iterator to the newly added element.
		auto iter = std::prev(list_.end());
		// Store iterator in the map
		map_[item] = iter;
	}

	// gets first element of structure
	index get() {
		if (list_.empty()) {
			throw std::out_of_range("IndexStorage is empty");
		}
		return list_.front();
	}

	bool empty() const {
		return list_.empty();
	}

private:
	std::list<index> list_;                                     // Maintains the order of insertion.
	std::unordered_map<index, std::list<index>::iterator> map_; // Maps index to list iterator.
};

/*
  Buffer pool is used to work on persistent data that is stored for each table
  for start we will support write && reading in iterative way, there is no concept of transaction
  && There will be N worker thread and each worker will pin single page at most,
  For now assume there always will be some unpined pages to be used.
  After page is unpined, it can be removed from buffer pool, if it's dirty it must be flushed
  to the disk

  we use single mutex for evictions and getting pages from free pages list
  and use per page mutex for per page metadata
*/

#define PAGE_COUNT 1024

struct InMemoryPage {
	void Reset() {
		std::memset(data_, 0, PageSize);
	}

	char *GetData() {
		return this->data_;
	};

private:
	char data_[PageSize];
};

/* Current state of page,
  FREE -> state can be changed to either UNIQUE_LOCKED or SHARED_LOCKED
  UNIQUE_LOCKED -> state can be changed to DISK_OP
  SHARED_LOCKED -> state can be changed to FREE
*/
enum class STATE { FREE, UNIQUE_LOCKED, SHARED_LOCKED };

struct PageMetadata {
	PageMetadata(InMemoryPage *page, index in_memory_pid) : mempage(page), in_memory_index(in_memory_pid) {
	}
	InMemoryPage *mempage;
	const index in_memory_index;

	STATE state;
	index disk_index;
	std::atomic<unsigned int> pin_count;
	bool is_dirty;
	std::shared_mutex page_mutex;
	std::promise<bool> promise_;
};

class DiskManager;

class BufferPool {
public:
	friend class DiskManager;

	/** @brief Create bufferpool, allocate page_count * PageSize of memory */
	BufferPool();

	~BufferPool();

	/** @brief Shut down buffer pool
	 * since disk manager will be closed first and will take
	 * care of writing diry indexes here we will just deallocate memory
	 */

	/**  @brief Assign disk, this buffer pool will use to write pages to
	 * return true on succes, false on nullptr or if dm_ is already set
	 */
	bool SetDisk(DiskManager *dm);

	/**
	 * @brief Tries to acquire shared lock on given on_disk page
	 * if if'ts already in memory in shared lock or not in buffer pool
	 * loads it and sets in_memory_pid to page that holds it
	 * @return return true on succes otherwise returns false
	 */
	bool GetPageReadonly(index *in_memory_pid, const index &on_disk_pid);

	/**
	 * @brief Tries to acquire exclusive lock on given on_disk and read if from disk
	 * @return return true on succes otherwise returns false
	 */
	bool GetPageWriteable(index *in_memory_pid, const index &on_disk_pid);

	/**
	@brief  Creates new page on disk and acquire Exlusive lock to page that holds it
  , resize disk if needed,
	sets values of on disk page index in in_memory_page metadata
	  of returned page
	 * @return return in memory index of allocated page
	*/
	index CreatePage();

	/** @brief free's page and unlock held lock
	 * if it was held in exlusive lock, write's it to the disk
	 * if it was held in shared lock decreses pin count
	 * and only truly free's it when it reaches 0
	 */
	void UnpinPage(const index &in_memory_pid);

	/**
	 * @bried delete's page with given id from disk
	 * @return true on success false if page doesn't exists
	 */
	bool DeletePage(const index &in_memory_pid);

	/**  @return readonly raw buffer content of given in memory page*/
	const char *GetDataReadonly(const index &in_memory_pid);

	/**  @return writeable raw buffer content of given in memory page*/
	char *GetDataWriteable(const index &in_memory_pid);

	index GetDiskIndex(const index &in_memory_pid);

private:
	/* always return but might wait
	  assume caller holds buffer_mutex_ locked
	  @return id of clean buffer_pool_page
	*/
	index GetFreePage(std::unique_lock<std::mutex> &buffer_lock);

	const size_t GetPageCount() {
		return this->page_count_;
	}

	DiskManager *disk_manager_;

	const size_t page_count_;

	unsigned int DiskIndex;

	std::array<std::shared_ptr<PageMetadata>, PAGE_COUNT> pages_metadata_;

	std::map<index, index> disk_to_memory_mapping_;

	// this mutex is needed to be held for
	// evictions,
	// getting pages from free pages list
	std::mutex buffer_mutex_;

	// std::set<index> free_pages_;
	IndexStorage free_pages_;
	std::condition_variable new_free_page_;

	// stored here only to call free on it
	char *const mem_buff_;
};

} // namespace AliceDB

#endif