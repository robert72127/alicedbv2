#ifndef ALICEDBCACHE
#define ALICEDBCACHE

#include "Common.h"

#include <unordered_map>
#include <vector>

namespace AliceDB {

#define DEFAULT_CACHE_SIZE (200)

template <typename Type>
std::array<char, sizeof(Type)> Key(const Type &type) {
	std::array<char, sizeof(Type)> key;
	std::memcpy(key.data(), &type, sizeof(Type));
	return key;
}

template <typename Type>
struct KeyHash {
	std::size_t operator()(const std::array<char, sizeof(Type)> &key) const {
		// You can use any suitable hash algorithm. Here, we'll use std::hash with
		// std::string_view
		return std::hash<std::string_view>()(std::string_view(key.data(), key.size()));
	}
};

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
		std::array<char, sizeof(Type)> key = current_->first;
		std::memcpy(&out_tuple.data, key.data(), sizeof(Type));
		out_tuple.delta = current_->second[delta_index_];
		delta_index_++;
		return out_tuple;
	}

	void Insert(const Tuple<Type> &tpl) {
		auto key = Key(tpl.data);
		tuples_[key].push_back(tpl.delta);
	}

	void Insert(const Type &data, const Delta &delta) {
		auto key = Key(data);
		tuples_[key].push_back(delta);
	}

private:
	void ResetIteration() {
		current_ = tuples_.begin();
		delta_index_ = 0;
	}

	// current operation
	size_t tuple_index_;
	size_t delta_index_;
	typename std::unordered_map<std::array<char, sizeof(Type)>, std::vector<Delta>>::iterator current_;

	// storage

	std::unordered_map<std::array<char, sizeof(Type)>, std::vector<Delta>, KeyHash<Type>> tuples_;
};

} // namespace AliceDB

#endif