#ifndef ALICEDBCACHE
#define ALICEDBCACHE

#include "Common.h"

#include <unordered_map>
#include <vector>

namespace AliceDB {

template <typename Type>
class Cache {
public:
	Cache(size_t initial_size) {
		tuples_.reserve(initial_size);
	}

	void Clean() {
		this->tuples_.clear();
		this->ResetIteration();
	}

	bool HasNext() {
		if (current_ == tuples_.end()) {
			this->ResetIteration();
			return false;
		}
		if (delta_index_ < current_->second.size())
			return true;
		current_++;
		delta_index_ = 0;
		return HasNext();
	}

	Tuple<Type> GetNext() {
		Tuple<Type> out_tuple;
		out_tuple.data = current_->first;
		out_tuple.delta = current_->second[delta_index_];
		delta_index_++;
		return out_tuple;
	}

	void Insert(const Type &data, const Delta &delta) {
		tuples_[data].push_back(delta);
	}

private:
	void ResetIteration() {
		current_ = tuples_.begin();
		delta_index_ = 0;
	}

	// current operation
	size_t tuple_index_;
	size_t delta_index_;
	typename std::unordered_map<Type, std::vector<Delta>>::iterator current_;

	// storage
	std::unordered_map<Type, std::vector<Delta>> tuples_;
};

} // namespace AliceDB

#endif