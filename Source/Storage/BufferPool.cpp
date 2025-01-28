#include "Storage/BufferPool.h"

#include "Storage/DiskManager.h"

#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

namespace AliceDB {

BufferPool::BufferPool()
    : mem_buff_ {static_cast<char *>(std::aligned_alloc(PageSize, PageSize * page_count_))}, page_count_(PAGE_COUNT) {

	memset(this->mem_buff_, 0, PageSize * page_count_);
	char *buffer = this->mem_buff_;
	for (size_t i = 0; i < this->page_count_; i++) {
		InMemoryPage *mempage = new (buffer) InMemoryPage();
		this->pages_metadata_[i] = std::make_shared<PageMetadata>(mempage, i);
		this->pages_metadata_[i]->disk_index = -1;
		this->pages_metadata_[i]->state = STATE::FREE;
		buffer += PageSize;
	}

	// emplace all page indexes to empty pages list
	for (int i = 0; i < this->page_count_; i++) {
		this->free_pages_.insert(i);
	}
}

BufferPool::~BufferPool() {
	free(this->mem_buff_);
	this->disk_manager_ = nullptr;
}

bool BufferPool::SetDisk(DiskManager *dm) {
	if (dm == nullptr) {
		return false;
	}
	this->disk_manager_ = dm;
	return true;
}

bool BufferPool::GetPageReadonly(index *in_memory_pid, const index &on_disk_pid) {
	std::unique_lock<std::mutex> buffer_lock(this->buffer_mutex_);

	if (this->disk_to_memory_mapping_.contains(on_disk_pid)) {

		index page_index = this->disk_to_memory_mapping_[on_disk_pid];
		std::shared_ptr<PageMetadata> pm = this->pages_metadata_[page_index];

		pm->page_mutex.lock_shared();
		// ok increase pin count and return page index
		pm->pin_count++;
		pm->state = STATE::SHARED_LOCKED;

		if (this->free_pages_.contains(page_index)) {
			this->free_pages_.erase(page_index);
		}

		*in_memory_pid = page_index;

		return true;
	}

	// ok page is not present in bufferpool

	// first get free page from free_pages_
	*in_memory_pid = this->GetFreePage(buffer_lock);
	this->disk_to_memory_mapping_[on_disk_pid] = *in_memory_pid;
	std::shared_ptr<PageMetadata> pm = this->pages_metadata_[*in_memory_pid];
	pm->state = STATE::SHARED_LOCKED;
	pm->disk_index = on_disk_pid;

	// unlock buffer lock to allow disk manager to acces it
	// but also lock exclusive page_lock so that no other page will acquire it in between
	pm->page_mutex.lock();
	buffer_lock.unlock();

	std::future<bool> result = this->disk_manager_->ReadPage(*in_memory_pid);
	if (result.get() == false) {
		UnpinPage(*in_memory_pid);
		return false;
	}

	// downgrade page_lock to shared
	buffer_lock.lock();
	pm->page_mutex.unlock();
	pm->page_mutex.lock_shared();

	return true;
}

bool BufferPool::GetPageWriteable(index *in_memory_pid, const index &on_disk_pid) {
	std::unique_lock<std::mutex> buffer_lock(this->buffer_mutex_);

	if (this->disk_to_memory_mapping_.contains(on_disk_pid)) {

		index page_index = this->disk_to_memory_mapping_[on_disk_pid];
		std::shared_ptr<PageMetadata> pm = this->pages_metadata_[page_index];

		pm->page_mutex.lock();
		// ok increase pin count and return page index
		pm->pin_count = 1;
		pm->state = STATE::UNIQUE_LOCKED;

		// remove it from free pages if it's present
		if (this->free_pages_.contains(page_index)) {
			this->free_pages_.erase(page_index);
		}

		*in_memory_pid = page_index;

		return true;
	}

	// ok page is not present in bufferpool

	// first get free page from free_pages_
	*in_memory_pid = this->GetFreePage(buffer_lock);
	this->disk_to_memory_mapping_[on_disk_pid] = *in_memory_pid;
	std::shared_ptr<PageMetadata> pm = this->pages_metadata_[*in_memory_pid];
	pm->state = STATE::UNIQUE_LOCKED;
	pm->disk_index = on_disk_pid;

	// unlock buffer lock to allow disk manager to acces it
	// but also lock exclusive page_lock so that no other page will acquire it in between
	pm->page_mutex.lock();
	buffer_lock.unlock();

	std::future<bool> result = this->disk_manager_->ReadPage(*in_memory_pid);
	if (result.get() == false) {
		UnpinPage(*in_memory_pid);
		return false;
	}

	return true;
}

index BufferPool::CreatePage() {
	// first get free page
	std::unique_lock<std::mutex> buffer_lock(this->buffer_mutex_);
	index in_memory_pid = this->GetFreePage(buffer_lock);

	std::shared_ptr<PageMetadata> pm = this->pages_metadata_[in_memory_pid];
	pm->page_mutex.lock();

	// set disk index to newly allocated page
	pm->disk_index = this->disk_manager_->AllocatePage();
	pm->is_dirty = true;
	pm->state = STATE::UNIQUE_LOCKED;

	this->disk_to_memory_mapping_.insert({pm->disk_index, in_memory_pid});

	return in_memory_pid;
}

void BufferPool::UnpinPage(const index &in_memory_pid) {

	std::shared_ptr<PageMetadata> pm = this->pages_metadata_[in_memory_pid];
	if (pm->state == STATE::UNIQUE_LOCKED) {
		std::future<bool> result = this->disk_manager_->WritePage(in_memory_pid);
		pm->pin_count = 0;
		pm->is_dirty = false;
		pm->state = STATE::FREE;
		result.get();

		pm->page_mutex.unlock();
		// before we acquired buffer_lock page might have been already grabbed by
		// other thread waiting on this specific page, if that was the case it increased pin count
		// then we don't want to put in on free list
		// it's not possible for it to be acquired by thread waiting in GetFreePages since it wasn't placed on that list
		// yet
		std::unique_lock<std::mutex> buffer_lock(this->buffer_mutex_);
		if (pm->pin_count == 0) {
			free_pages_.insert(in_memory_pid);
			this->new_free_page_.notify_all();
		}

	} else if (pm->state == STATE::SHARED_LOCKED || pm->state == STATE::FREE) {
		pm->pin_count--;
		if (pm->pin_count == 0) {
			pm->is_dirty = false;
			pm->state = STATE::FREE;
		}

		pm->page_mutex.unlock();

		if (pm->pin_count == 0) {
			// another three are  there
			// before we acquired buffer_lock page might have been already grabbed by
			// other thread waiting on this specific page, if that was the case it increased pin count
			// then we don't want to put in on free list
			// it's not possible for it to be acquired by thread waiting in GetFreePages since it wasn't placed on that
			// list yet

			std::unique_lock<std::mutex> buffer_lock(this->buffer_mutex_);
			if (pm->pin_count == 0) {
				free_pages_.insert(in_memory_pid);
				this->new_free_page_.notify_all();
			}
		}
	}
}

bool BufferPool::DeletePage(const index &in_memory_pid) {
	std::unique_lock<std::mutex> buffer_lock(this->buffer_mutex_);

	std::shared_ptr<PageMetadata> pm = this->pages_metadata_[in_memory_pid];
	if (pm->pin_count != 1) {
		return false;
	}

	this->disk_manager_->DeletePage(pm->disk_index);
	pm->disk_index = -1;
	pm->is_dirty = false;
	pm->pin_count = 0;
	pm->mempage->Reset();
	this->free_pages_.insert(in_memory_pid);

	this->new_free_page_.notify_all();

	return true;
}

const char *BufferPool::GetDataReadonly(const index &in_memory_pid) {
	std::shared_ptr<PageMetadata> pm = this->pages_metadata_[in_memory_pid];
	if (pm->state == STATE::FREE) {
		return nullptr;
	}
	return const_cast<char *>(pm->mempage->GetData());
}

char *BufferPool::GetDataWriteable(const index &in_memory_pid) {
	std::shared_ptr<PageMetadata> pm = this->pages_metadata_[in_memory_pid];
	if (pm->state != STATE::UNIQUE_LOCKED) {
		return nullptr;
	}
	return pm->mempage->GetData();
}

index BufferPool::GetFreePage(std::unique_lock<std::mutex> &buffer_lock) {

	while (true) {
		if (!this->free_pages_.empty()) {
			// simple least recently used :)
			index in_memory_pid = this->free_pages_.get();
			free_pages_.erase(in_memory_pid);

			std::shared_ptr<PageMetadata> pm = this->pages_metadata_[in_memory_pid];
			if (pm->pin_count != 0) {
				continue;
			}

			this->disk_to_memory_mapping_.erase(pm->disk_index);

			pm->pin_count = 1;
			pm->disk_index = -1;
			pm->is_dirty = false;
			pm->mempage->Reset();
			pm->state = STATE::FREE;

			return in_memory_pid;
		}

		// by design other pages are pinned so we have to wait
		// for either disk manager to finish write's and notifies or for readonly page to be free'd
		this->new_free_page_.wait(buffer_lock, [this] { return !this->free_pages_.empty(); });
	}
}

index BufferPool::GetDiskIndex(const index &in_memory_pid) {
	std::shared_ptr<PageMetadata> pm = this->pages_metadata_[in_memory_pid];
	return pm->disk_index;
}

} // namespace AliceDB