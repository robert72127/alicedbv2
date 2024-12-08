#ifndef ALICEDBQUEUE
#define ALICEDBQUEUE

#include "Tuple.h"
#include "Common.h"

#include <new>
#include <cstdlib>
#include <cstring>

namespace AliceDB {

struct Queue {

	/**
	 * @brief create queue with given size of queue_size * tuple_size where
	 * iterator jumps by tuple_size
	 * tuple size is size of QueueTuple<Type> size but we make it untyped on purpouse, for easier chaining
	 * using abstract base class
	 */
	Queue(size_t queue_size, size_t tuple_size)
	    : tuple_size_ {tuple_size}, queue_size_ {queue_size}, current_size_ {0},
	      current_offset_ {0}, storage_ {new char[queue_size * tuple_size]} {
	}

	/**
	 * @brief deallocate queue memory
	 */
	~Queue() {
		delete[] storage_;
	}

	/**
	 * @brief insert new element into queue
	 * @return true on success, false if there is no space in queue left
	 */
	void Insert(const char *data) {
		if (this->current_size_ + this->tuple_size_ > this->queue_size_) {
			// just resize :)
			this->Resize(current_size_ * 2);
		}
		std::memcpy(this->storage_ + this->current_size_, data, this->tuple_size_);
		this->current_size_ += tuple_size_;
	}

	/**
	 * this is for writing
	 * @brief  reserve next free position in queue,
	 * set data to point to next free position
	 * and assume user will insert's data there thus
	 * increase metadata as in Insert
	 */
	void ReservetNext(char **data) {
		if (this->current_size_ + this->tuple_size_ > this->queue_size_) {
			this->Resize(current_size_ * 2);
		}
		*data = this->storage_ + this->current_size_;
		this->current_size_ += tuple_size_;
	}
	/**
	 * @brief set's queue memory to 0
	 * and reset internal state
	 */
	void Clean() {
		std::memset(this->storage_, 0, queue_size_);
		this->current_offset_ = 0;
		this->current_size_ = 0;
	}
	/**
	 * this is used for reading
	 * set tuple to next tuple in queue,
	 * @return true if there are tuples left
	 * false otherwise, then also reset current_offset to allow for new iteration
	 */
	bool GetNext(char **tuple) {
		if (this->current_offset_ + this->tuple_size_ > this->queue_size_) {
			this->current_offset_ = 0;
			return false;
		}
		char *current = this->storage_ + current_offset_;
		current_offset_ += tuple_size_;
		*tuple = this->storage_ + this->current_offset_;
		return true;
	}

	// get index'th tuple
	char *Get(index index) {
		return (this->storage_ + index * this->tuple_size_);
	}


	char *storage_;
	size_t queue_size_;
	size_t tuple_size_;
	// used for insterting
	size_t current_size_;
	// used for GetNext
	size_t current_offset_;

private:
	bool Resize(size_t new_size) {
		if (new_size <= this->queue_size_) {
			// No point in resizing to something smaller or equal
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
		this->queue_size_ = new_size;
		return true;
	}
};

} // namespace AliceDB

#endif