#ifndef ALICEDBCACHE
#define ALICEDBCACHE

#include "Common.h"
#include "Tuple.h"

#include <cstdlib>
#include <cstring>
#include <new>

namespace AliceDB {

struct Cache {
	/**
	 * @brief create cache with given size of cache_size * tuple_size where
	 * iterator jumps by tuple_size
	 * tuple size is size of cacheTuple<Type> size but we make it untyped on
	 * purpouse, for easier chaining using abstract base class
	 */
	Cache(size_t cache_size, size_t tuple_size)
	    : tuple_size_ {tuple_size}, cache_size_ {cache_size}, current_size_ {0},
	      current_offset_ {0}, storage_ {new char[cache_size * tuple_size]} {
	}

	/**
	 * @brief deallocate cache memory
	 */
	~Cache() {
		delete[] storage_;
	}

	/**
	 * @brief insert new element into cache
	 * @return true on success, false if there is no space in cache left
	 */
	void Insert(const char *data) {
		if (this->current_size_ + this->tuple_size_ > this->cache_size_) {
			// just resize :)
			this->Resize(current_size_ * 2);
		}
		std::memcpy(this->storage_ + this->current_size_, data, this->tuple_size_);
		this->current_size_ += tuple_size_;
	}

	/**
	 * this is for writing
	 * @brief  reserve next free position in cache,
	 * set data to point to next free position
	 * and assume user will insert's data there thus
	 * increase metadata as in Insert
	 */
	void ReserveNext(char **data) {
		if (this->current_size_ + this->tuple_size_ > this->cache_size_) {
			this->Resize(current_size_ * 2);
		}
		*data = this->storage_ + this->current_size_;
		this->current_size_ += tuple_size_;
	}
	/**
	 * @brief set's cache memory to 0
	 * and reset internal state
	 */
	void Clean() {
		this->last_10_max_size_ =
		    this->last_10_max_size_ > this->current_size_ ? this->last_10_max_size_ : this->current_size_;
		this->check_resize_++;
		if (this->check_resize_ > 9) {
			if (this->cache_size_ > this->last_10_max_size_) {
				this->Resize(last_10_max_size_);
			}
			this->check_resize_ = 0;
		}

		std::memset(this->storage_, 0, cache_size_);
		this->current_offset_ = 0;
		this->current_size_ = 0;
	}
	/**
	 * this is used for reading
	 * set tuple to next tuple in cache,
	 * @return true if there are tuples left
	 * false otherwise, then also reset current_offset to allow for new iteration
	 */
	bool GetNext(const char **tuple) {
		if (this->current_offset_ + this->tuple_size_ > this->current_size_) {
			this->current_offset_ = 0;
			return false;
		}
		*tuple = this->storage_ + this->current_offset_;
		current_offset_ += tuple_size_;
		return true;
	}

	// get index'th tuple
	char *Get(index index) {
		return (this->storage_ + index * this->tuple_size_);
	}

	void RemoveLast() {
		current_size_ -= tuple_size_;
	}

	char *storage_;
	size_t cache_size_;
	size_t tuple_size_;
	// used for insterting
	size_t current_size_;
	// used for GetNext
	size_t current_offset_;

private:
	bool Resize(size_t new_size) {
		// Resize down only from statistics
		if (new_size <= this->cache_size_ && this->check_resize_ < 9) {
			return false;
		}

		char *new_storage = new (std::nothrow) char[new_size];
		if (!new_storage) {
			return false;
		}

		// Copy existing data
		std::memcpy(new_storage, this->storage_, this->current_size_);

		// Clean up old storage
		delete[] this->storage_;

		// Update pointers and size
		this->storage_ = new_storage;
		this->cache_size_ = new_size;
		return true;
	}
	// statistics
	// if we have statistics from last 10 usages, resize to max size of those if it's smaller than
	// current one
	size_t last_10_max_size_;
	int check_resize_;
};

} // namespace AliceDB

#endif