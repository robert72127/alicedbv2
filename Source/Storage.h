/*
We need to store following things:

<Index > list of pairs <timestamp| count> sorted by timestamp - this is handled by delta storage

<Key(Tuple) | index> - index is naturally computed based on tuple location in heap file,
but's its also stored in searchtree(couldn't find better name) and used to acces deltas

<Match | Keys> - recomputed on the fly when restarting the system, also stored in search tree

classes below can be designed for single thread since we work on single node by one thread at once
*/

#ifndef ALICEDBSTORAGE
#define ALICEDBSTORAGE

#include "BufferPool.h"
#include "City.h"
#include "Common.h"
#include "TablePage.h"

#include <filesystem>
#include <fstream>
#include <functional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace AliceDB {

class Graph;

/**
 * Storage for <index | delta > mappings
 * main idea is to store structure as stl container in memory and later overwrite log file on disk on compression op
 *
 */
class DeltaStorage {
public:
	template <typename Type, typename MatchType>
	friend class Table;

	/*initialize delta storage from log file*/
	DeltaStorage(std::string log_file) : log_file_ {log_file} {
		if (this->ReadLogFile()) {
			// corrupted data, clean in memory stuff
			deltas_.clear();
		}
	}

	~DeltaStorage() {
		this->UpdateLogFile();
	}

	/**
	 * @brief insert new delta into the table
	 * @return true if index wasn't present
	 * false otherwise
	 */
	bool Insert(const index idx, const Delta &d) {
		// get correct index
		if (!this->deltas_.contains(idx)) {
			this->deltas_[idx] = {d};
			// index wasn't present return false
			return false;
		} else {
			// index was preset return true
			auto &vec = deltas_[idx];
			auto pos = std::upper_bound(vec.begin(), vec.end(), d, DeltaComparator());
			vec.insert(pos, d);
			return true;
		}
	}

	void Delete(const index idx) {
		deltas_.erase(idx);
	}

	/**
	 * @brief merge tuples from oldest up to end_ts by summing values, by index for given table up to end_timestamp
	 */

	void Merge(const timestamp end_ts) {
		for (auto &[idx, deltas] : deltas_) {
			Delta ins_delta = {0, 0};
			bool first = true;
			for (auto it = deltas.begin(); it != deltas.end();) {
				if (it->ts > end_ts) {
					if (!first)
						deltas.emplace(deltas.begin(), ins_delta);
					break;
				} else {
					ins_delta.count += it->count;
					ins_delta.ts = it->ts;
					it = deltas.erase(it);
					if (it == deltas.end()) {
						deltas.emplace(deltas.begin(), ins_delta);
						break;
					}
					first = false;
				}
			}
		}
		UpdateLogFile();
	}

	/**
	 * @brief returns multiset of all the deltas for given key
	 */
	inline const std::vector<Delta> &Scan(const index idx) {
		return this->deltas_[idx];
	}

	// return oldest delta for index
	inline Delta Oldest(index idx) {
		return deltas_[idx][0];
	}

	inline size_t Size() {
		return this->deltas_.size();
	}

	std::unordered_map<index, std::vector<Delta>> &Get() {
		return this->deltas_;
	}

private:
	// Write the entire delta storage as a binary file.
	void UpdateLogFile() {
		std::string tmp_filename = log_file_ + "_tmp";
		std::ofstream file_stream(tmp_filename, std::ios::binary | std::ios::out | std::ios::trunc);
		if (!file_stream)
			throw std::runtime_error("Failed to open temporary log file for writing");

		// for each Delta
		for (auto &[idx, deltas] : deltas_) {
			// write key
			file_stream.write(reinterpret_cast<const char *>(&idx), sizeof(idx));
			// write number of deltas
			std::size_t vecSize = deltas.size();
			file_stream.write(reinterpret_cast<const char *>(&vecSize), sizeof(vecSize));
			// Write each deltas's fields.
			for (const auto &d : deltas) {
				file_stream.write(reinterpret_cast<const char *>(&d.count), sizeof(d.count));
				file_stream.write(reinterpret_cast<const char *>(&d.ts), sizeof(d.ts));
			}
		}
		file_stream.close();
		std::filesystem::rename(tmp_filename, log_file_);
	}

	// read the binary log file and reconstruct the in-memory storage.
	int ReadLogFile() {
		std::ifstream file_stream(log_file_, std::ios::binary | std::ios::in);
		// no log, ok it's first time
		if (!file_stream) {
			return 0;
		}

		deltas_.clear();
		while (file_stream.peek() != EOF) {
			index idx;
			std::size_t num_deltas;

			// read idx
			file_stream.read(reinterpret_cast<char *>(&idx), sizeof(idx));
			if (!file_stream)
				break;
			// deltas cnt
			file_stream.read(reinterpret_cast<char *>(&num_deltas), sizeof(num_deltas));
			if (!file_stream) {
				return 1;
			}

			std::vector<Delta> deltas;
			deltas.resize(num_deltas);
			for (std::size_t i = 0; i < num_deltas; i++) {
				file_stream.read(reinterpret_cast<char *>(&deltas[i].count), sizeof(deltas[i].count));
				file_stream.read(reinterpret_cast<char *>(&deltas[i].ts), sizeof(deltas[i].ts));
				if (!file_stream) {
					return 1;
				}
			}
			deltas_[idx] = std::move(deltas);
		}
		return 0;
	}

	// multiset of deltas for each index
	std::unordered_map<index, std::vector<Delta>> deltas_;
	std::string log_file_;
};

struct StorageIndex {
	index page_id_;
	index tuple_id_;

	bool operator<(const StorageIndex &other) const {
		if (this->page_id_ == other.page_id_) {
			return this->tuple_id_ < other.tuple_id_;
		}
		return this->page_id_ < other.page_id_;
	}

	bool operator==(const StorageIndex &other) const {
		return this->page_id_ == other.page_id_ && this->tuple_id_ == other.tuple_id_;
	}
};

template <typename Type>
struct HeapState {
	const Type *data_;
	index idx_;
};
/** @brief iterator-like class for iterating all tuples on heap,
 * instead of * it uses Get method which return tuple's data part, and table interal index corresponding to it */
template <typename Type>
class HeapIterator {
public:
	HeapIterator(index *page_idx, index tpl_idx, BufferPool *bp, unsigned int tuples_per_page, size_t pages_count)
	    : page_idx_ {page_idx}, tpl_idx_ {tpl_idx}, bp_ {bp}, tuples_per_page_ {tuples_per_page},
	      pages_count_ {pages_count}, start_page_idx_ {page_idx} {
	}

	HeapIterator(const HeapIterator &other)
	    : page_idx_ {other.page_idx_}, tpl_idx_ {other.tpl_idx_}, bp_ {other.bp_},
	      tuples_per_page_ {other.tuples_per_page_}, pages_count_ {other.pages_count_}, start_page_idx_ {
	                                                                                        other.start_page_idx_} {
	}

	HeapState<Type> Get() {
		this->LoadPage();
		// when returning heap state we want to return page index corresponding to position in vector of pages, not
		// actual on disk page id
		index logical_page_index = (page_idx_ - start_page_idx_);
		return HeapState<Type>(current_page_->Get(tpl_idx_), this->tuples_per_page_ * logical_page_index + tpl_idx_);
	}

	HeapIterator &operator++() {

		this->LoadPage();

		this->tpl_idx_++;

		while (!this->current_page_->Contains(tpl_idx_)) {

			if (this->tpl_idx_ == this->tuples_per_page_) {
				this->tpl_idx_ = 0;
				this->page_idx_++;
				// we reached end, thus we won't find tuple and must return
				if (this->page_idx_ == this->start_page_idx_ + this->pages_count_) {
					return *this;
				}
			} else {
				this->tpl_idx_++;
			}
		}

		return *this;

	} // Prefix increment

	void LoadPage() {
		if (!this->current_page_ || this->current_page_->disk_index_ != *this->page_idx_) {
			this->current_page_ =
			    std::make_unique<TablePageReadOnly<Type>>(this->bp_, *this->page_idx_, this->tuples_per_page_);
		}
	}

	bool operator!=(const HeapIterator &other) const {
		return page_idx_ != other.page_idx_ || tpl_idx_ != other.tpl_idx_;
	}

private:
	index *page_idx_;
	index tpl_idx_;

	BufferPool *bp_;
	size_t tuples_per_page_;

	size_t pages_count_;
	index *start_page_idx_;

	std::unique_ptr<TablePageReadOnly<Type>> current_page_;
};

template <typename Type, typename MatchType>
class Table;

template <typename Type>
Type Identity(const Type &input) {
	return input;
}

/**
 * @brief class that is used for fast search on whether given key already exists in database,
 * it stores hash of the key with index as value, to confirm tuple is actually present in persistent storage,
 * and presence of key is not effect of colission it reads page from disk to confirm
 */
template <typename Type, typename TableType, typename MatchType>
struct SearchTree {

	SearchTree(Table<Type, TableType> *table, std::function<MatchType(const Type &)> transform)
	    : table_ {table}, transform_ {transform}, key_size_ {sizeof(MatchType)} {
		// init tuples to tables from heap iterator
		for (auto it = this->table_->begin(); it != this->table_->end(); ++it) {
			auto [data, idx] = it.Get();
			this->Insert(*data, this->table_->IndexToStorageIndex(idx));
		}
	}

	// return true if key was not present, else returns false, sets idx to corresponding index
	void Insert(const Type &key, const StorageIndex &idx) {
		MatchType match_key = this->transform_(key);
		uint64 key_hash = CityHash64WithSeed((char *)&match_key, this->key_size_, 0);

		if (!this->tuples_to_index_.contains(key_hash)) {
			this->tuples_to_index_[key_hash] = {};
		}
		auto &vec = this->tuples_to_index_[key_hash];
		auto pos = std::upper_bound(vec.begin(), vec.end(), idx);
		vec.insert(pos, idx);
	}

	// searches for key if it finds it sets idx to corresponding index, and returns true,
	// else returns alse
	bool Search(const Type &key, StorageIndex &idx) {
		MatchType match_key = this->transform_(key);
		uint64 key_hash = CityHash64WithSeed((char *)&match_key, this->key_size_, 0);
		auto &candidates = this->tuples_to_index_[key_hash];
		for (const auto &candidate_idx : candidates) {
			Type candidate = this->table_->Get(this->table_->StorageIndexToIndex(candidate_idx));
			if (std::memcmp((char *)&candidate, (char *)&match_key, this->key_size_) == 0) {
				idx = candidate_idx;
				return true;
			}
		}
		return false;
	}

	// searches for  key if it finds it returns all tuples that corresponds to match_key
	std::vector<std::pair<Type, index>> MatchSearch(const MatchType &match_key) {
		std::vector<std::pair<Type, index>> matching_idx;
		uint64 key_hash = CityHash64WithSeed((char *)&match_key, this->key_size_, 0);
		auto &candidates = this->tuples_to_index_[key_hash];
		for (const auto &candidate_idx : candidates) {
			index tuple_idx = this->table_->StorageIndexToIndex(candidate_idx);
			Type candidate = this->table_->Get(tuple_idx);
			MatchType match_candidate = this->transform_(candidate);
			if (std::memcmp((char *)&match_candidate, (char *)&match_key, this->key_size_) == 0) {
				matching_idx.push_back({candidate, tuple_idx});
			}
		}
		return matching_idx;
	}

	// deletes tuple from storage
	bool Delete(const Type &key, StorageIndex str_idx) {
		MatchType match_key = this->transform_(key);
		uint64 key_hash = CityHash64WithSeed((char *)&match_key, this->key_size_, 0);

		auto &vec = this->tuples_to_index_[key_hash];
		for (auto it = vec.begin(); it != vec.end(); it++) {
			if (*it == str_idx) {
				it = vec.erase(it);
				return true;
			}
		}
		return false;
	}

private:
	Table<Type, TableType> *table_;
	size_t key_size_;
	// std::vector<index> btree_page_indexes_;
	std::unordered_map<uint64_t, std::vector<StorageIndex>> tuples_to_index_ = {};

	std::function<MatchType(const Type &)> transform_;
};

/**
 * General storage class that tracks deltas, and tuples
 */
template <typename Type, typename MatchType = Type>
class Table {
public:
	Table(std::string delta_storage_fname, std::vector<index> &data_page_indexes, std::vector<index> &btree_indexes,
	      BufferPool *bp, Graph *g)
	    : bp_ {bp}, g_ {g}, ds_ {std::make_unique<DeltaStorage>(delta_storage_fname)},
	      data_page_indexes_ {data_page_indexes}, btree_indexes_ {btree_indexes},
	      tuples_per_page_ {PageSize / (sizeof(bool) + sizeof(Type))}, use_match_to_index_ {false} {

		this->tree_ = new SearchTree<Type, MatchType, Type>(this, Identity<Type>);
	}

	Table(std::string delta_storage_fname, std::vector<index> &data_page_indexes, std::vector<index> &btree_indexes,
	      BufferPool *bp, Graph *g, std::function<MatchType(const Type &)> transform)
	    : bp_ {bp}, g_ {g}, ds_ {std::make_unique<DeltaStorage>(delta_storage_fname)},
	      data_page_indexes_ {data_page_indexes}, btree_indexes_ {btree_indexes},
	      tuples_per_page_ {PageSize / (sizeof(bool) + sizeof(Type))}, use_match_to_index_ {true} {

		this->tree_ = new SearchTree<Type, MatchType, Type>(this, Identity<Type>);

		this->match_tree_ = new SearchTree<Type, MatchType, MatchType>(this, transform);
	}

	~Table() {
		delete tree_;
		if (this->use_match_to_index_) {
			delete match_tree_;
		}
	}

	// return index if data already present in table, doesn't insert but just return index
	index Insert(const Type &in_data) {

		// first check if already present
		index idx;
		if (this->Search(in_data, &idx)) {
			return idx;
		}

		// ok not present, write to the next write page
		std::unique_ptr<TablePage<Type>> write_page;
		if (this->data_page_indexes_.empty()) {
			write_page = std::make_unique<TablePage<Type>>(this->bp_, this->tuples_per_page_);
			this->data_page_indexes_.push_back(write_page->GetDiskIndex());
			write_page->Insert(in_data, &idx);
			this->tree_->Insert(in_data,
			                    this->IndexToStorageIndex(idx + this->tuples_per_page_ * this->current_page_idx_));
			if (this->use_match_to_index_) {
				this->match_tree_->Insert(
				    in_data, this->IndexToStorageIndex(idx + this->tuples_per_page_ * this->current_page_idx_));
			}
			return idx + this->tuples_per_page_ * this->current_page_idx_;
		}

		write_page = std::make_unique<TablePage<Type>>(this->bp_, this->data_page_indexes_[this->current_page_idx_],
		                                               this->tuples_per_page_);

		// iterate to next pages to check if they have free space
		while (!write_page->Insert(in_data, &idx)) {
			this->current_page_idx_++;
			if (this->current_page_idx_ >= this->data_page_indexes_.size()) {
				break;
			}
			write_page = std::make_unique<TablePage<Type>>(this->bp_, this->data_page_indexes_[this->current_page_idx_],
			                                               this->tuples_per_page_);
		}

		if (this->current_page_idx_ == this->data_page_indexes_.size()) {
			// if theSearchre is no place left in current write page, allocate new one
			write_page = std::make_unique<TablePage<Type>>(this->bp_, this->tuples_per_page_);
			this->data_page_indexes_.push_back(write_page->GetDiskIndex());
			write_page->Insert(in_data, &idx);
		}

		this->tree_->Insert(in_data, this->IndexToStorageIndex(idx + this->tuples_per_page_ * this->current_page_idx_));
		if (this->use_match_to_index_) {
			this->match_tree_->Insert(
			    in_data, this->IndexToStorageIndex(idx + this->tuples_per_page_ * this->current_page_idx_));
		}

		return idx + this->tuples_per_page_ * this->current_page_idx_;
	}

	/**  @brief searches for data(key) in table using  if finds returns true and sets index value to found */
	bool Search(const Type &data, index *idx) {
		StorageIndex strg_idx;
		bool found = this->tree_->Search(data, strg_idx);
		if (found) {
			*idx = this->StorageIndexToIndex(strg_idx);
		}
		return found;
	}

	/** returns all indexes that matches hash of data in match tree*/
	std::vector<std::pair<Type, index>> MatchSearch(const MatchType &data) {
		if (!this->use_match_to_index_) {
			return {};
		}
		return this->match_tree_->MatchSearch(data);
	}

	/**
	 *
	 * @brief deletes all tuples that are older than ts,
	 * if zeros_only is set only those for which current delta count is zero are deleted
	 *
	 * */
	void GarbageCollect(timestamp ts, bool zeros_only) {
		// first we got to iterate delta storage to get vector of all the indexes we wish to delete
		std::vector<index> delete_indexes;

		for (auto it = this->ds_->deltas_.begin(); it != this->ds_->deltas_.end(); it++) {

			if (it->second.rbegin()->ts < ts) {

				if (zeros_only) {
					// calculate count and check if it's zero, then we can delete
					int sum = 0;
					for (const auto &dlt : it->second) {
						sum += dlt.count;
					}
					if (sum != 0) {
						continue;
					}
				}
				// ok since we are here we don't care about count, or cout is actually zero, so we can delete
				delete_indexes.emplace_back(it->first);
				// remove index from deltas
				it = this->ds_->deltas_.erase(it);
			}
		}

		// then when we get the indexes we should iterate pages and mark those tuples as deleted

		std::sort(delete_indexes.begin(), delete_indexes.end());
		std::unique_ptr<TablePage<Type>> wip_page_;
		for (const auto &idx : delete_indexes) {
			StorageIndex strg_idx = this->IndexToStorageIndex(idx);
			index page_idx = strg_idx.page_id_;
			index tpl_idx = strg_idx.tuple_id_;
			if (!wip_page_ || wip_page_->GetDiskIndex() != page_idx) {
				wip_page_ = std::make_unique<TablePage<Type>>(this->bp_, page_idx, this->tuples_per_page_);
			}
			// get key, and remove it from tree
			Type *Key = wip_page_->Get(tpl_idx);
			this->tree_->Delete(*Key, strg_idx);
			if (this->use_match_to_index_) {
				this->match_tree_->Delete(*Key, strg_idx);
			}
			wip_page_->Remove(tpl_idx);
		}

		// update new insert page to be the first page that had value deleted
		if (delete_indexes.size() > 0) {
			auto [page_idx, _] = this->IndexToStorageIndex(delete_indexes[0]);
			this->current_page_idx_ = page_idx;
		}

		// when we will insert new pages we will now consider first page that has holes, and then next ones.. till we
		// reach end, only then we will alloc new page
	}

	// return pointer to data corresponding to index, calculated using tuples per page & offset
	// if index is larger than tuple count returns nullptr
	Type Get(const index &idx) {
		StorageIndex str_idx = this->IndexToStorageIndex(idx);

		auto read_page = std::make_unique<TablePageReadOnly<Type>>(
		    this->bp_, this->data_page_indexes_[str_idx.page_id_], this->tuples_per_page_);
		Type tp;
		std::memcpy(&tp, read_page->Get(str_idx.tuple_id_), sizeof(Type));
		return tp;
	}

	HeapIterator<Type> begin() {
		return HeapIterator<Type>(this->data_page_indexes_.data(), 0, this->bp_, this->tuples_per_page_,
		                          this->data_page_indexes_.size());
	}

	HeapIterator<Type> end() {
		return HeapIterator<Type>(this->data_page_indexes_.data() + this->data_page_indexes_.size(), 0, bp_,
		                          this->tuples_per_page_, this->data_page_indexes_.size());
	}

	// methods to work with deltas
	bool InsertDelta(const index idx, const Delta &d) {
		return this->ds_->Insert(idx, d);
	}
	void MergeDelta(const timestamp end_ts) {
		return this->ds_->Merge(end_ts);
	}

	// returns all deltas for given index
	const std::vector<Delta> &Scan(const index idx) {
		return this->ds_->deltas_[idx];
	}

	std::unordered_map<index, std::vector<Delta>> &GetDeltas() {
		return this->ds_->Get();
	}

	// return oldest delta for index
	inline Delta OldestDelta(index idx) {
		return this->ds_->Oldest(idx);
	}

	inline size_t DeltasSize() {
		return this->ds_->Size();
	}

	inline index StorageIndexToIndex(StorageIndex sidx) {
		return this->tuples_per_page_ * sidx.page_id_ + sidx.tuple_id_;
	}

	StorageIndex IndexToStorageIndex(index idx) {
		return {idx / this->tuples_per_page_, idx % this->tuples_per_page_};
	}

private:
	// methods for page accesing etc

	// heap data pages
	unsigned int tuples_per_page_;

	std::vector<index> &data_page_indexes_;
	index current_page_idx_ = 0;

	std::vector<index> &btree_indexes_;

	SearchTree<Type, MatchType, Type> *tree_;

	const bool use_match_to_index_;
	SearchTree<Type, MatchType, MatchType> *match_tree_;

	// delta storage
	std::unique_ptr<DeltaStorage> ds_;

	// buffer pool pointer
	BufferPool *bp_;

	// graph pointer
	Graph *g_;
};

} // namespace AliceDB
#endif