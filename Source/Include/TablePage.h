#ifndef ALICEDBTABLEPAGE
#define ALICEDBTABLEPAGE

#include "BufferPool.h"
#include "Common.h"
#include "Tuple.h"

#include <cstddef>
#include <stdexcept>

namespace AliceDB {

/**  Per Table wrapper for BufferPool page
 *
 * there is single such structure per Table,
 * to get content of other page, just switch underlying using UpdateTable
 */

template <typename Type>
struct TablePage {

	/**
	 * @brief array like structure over BufferPool page,
	 * with switchable underlying page
	 * @todo optimize, use bit packing for slots */
	TablePage(BufferPool *bp, const index &on_disk_pid, size_t tuple_count) : bp_ {bp}, tuple_count_ {tuple_count} {
		if (!this->bp_->GetPageWriteable(&in_memory_pid_, on_disk_pid)) {
			throw std::runtime_error("Failed to get on disk page " + std::to_string(on_disk_pid) +
			                         ", into buffer pool");
		}

		char *data = this->bp_->GetDataWriteable(this->in_memory_pid_);
		this->slots_ = reinterpret_cast<bool *>(data);
		// for now each index is stored as byte, despite the fact that we only use one bit
		data += tuple_count * sizeof(bool);
		this->storage_ = data;

		this->disk_index_ = on_disk_pid;
	}

	TablePage(BufferPool *bp, size_t tuple_count) : bp_ {bp}, tuple_count_ {tuple_count} {

		index index = this->bp_->CreatePage();
		this->in_memory_pid_ = index;
		this->disk_index_ = this->bp_->GetDiskIndex(this->in_memory_pid_);


		char *data = this->bp_->GetDataWriteable(index);
		this->slots_ = reinterpret_cast<bool *>(data);
		// for now each index is stored as char, despie fact we only use one bit
		data += tuple_count;
		this->storage_ = data;
	}

	// free's locked page in bufferpool
	~TablePage() {
		this->slots_ = nullptr;
		this->storage_ = nullptr;
		this->bp_->UnpinPage(this->in_memory_pid_);
	}

	/** @brief removes data stored on given index in page*/
	void Remove(const index &id) {
		if (this->tuple_count_ + sizeof(Type) * id > PageSize) {
			throw std::runtime_error("Index doesn't exists");
		}
		this->slots_[id] = false;
		memset(this->storage_ + id, 0, sizeof(Type));
	}

	/** @brief Inserts tuple at first free index into table
	 * @return true on succesful insert, false if there is no space left,
	 */
	bool Insert(const Type &tuple, index *id) {
		for (int i = 0; i < this->tuple_count_; i++) {
			if (this->slots_[i] == 0) {
				this->slots_[i] = true;
				memcpy(this->storage_ + sizeof(Type) * i , &tuple, sizeof(Type));
				*id = i;
				return true;
			}
		}
		return false;
	}

	/** @brief return tuple stored at given index in Table */
	Type *Get(const index &id) {
		if (this->tuple_count_ + sizeof(Type) * id > PageSize) {
			throw std::runtime_error("Index doesn't exists");
		}
		return (Type *)(this->storage_ + id * sizeof(Type));
	}

	inline index GetDiskIndex() {
		return disk_index_;
	}

	// this is set by Table that own this struct
	BufferPool *bp_;
	size_t tuple_count_;
	index in_memory_pid_;
	index disk_index_;

	/* this is stored in actuall BufferPool page */
	bool *slots_;
	char *storage_;
};

/**  Per Table readonly wrapper for BufferPool page
 * there are two such structures per Table,
 * to get content of other page, just switch underlying using UpdateTable
 */
template <typename Type>
struct TablePageReadOnly {

	/**
	 * @brief array like structure over BufferPool page,
	 * with switchable underlying page
	 */
	TablePageReadOnly(BufferPool *bp, const index &on_disk_pid, size_t tuple_count)
	    : bp_ {bp}, tuple_count_ {tuple_count} {
		if (!this->bp_->GetPageReadonly(&in_memory_pid_, on_disk_pid)) {
			throw std::runtime_error("Failed to get on disk page " + std::to_string(on_disk_pid) +
			                         ", into buffer pool");
		}

		const char *data = this->bp_->GetDataReadonly(this->in_memory_pid_);
		this->slots_ = reinterpret_cast<const bool *>(data);
		// for now each index is stored as char, despie fact we only use one bit
		data += tuple_count;
		this->storage_ = data;
	}

	// unpin, this will also release held lock
	~TablePageReadOnly() {
		this->slots_ = nullptr;
		this->storage_ = nullptr;
		this->bp_->UnpinPage(this->in_memory_pid_);
	}

	/** @brief return tuple stored at given index in Table */
	const Type *Get(const index &id) {
		if (this->tuple_count_ + sizeof(Type) * id > PageSize) {
			throw std::runtime_error("Index doesn't exists");
		}

		return (Type *)(this->storage_ + id * sizeof(Type));
	}

	BufferPool *bp_;
	size_t tuple_count_;
	index in_memory_pid_;

	/* this is stored in actuall BufferPool page */
	const bool *slots_;
	const char *storage_;
};

} // namespace AliceDB

#endif
