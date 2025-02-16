/*
We need to store three things:

< Index | Timestamp > -> Count - this can either be done entirely in memory or also using btree, cause it gives us
prefix search for free

<Key(Tuple) | index> - this can be done by btree

<Match | Keys> - this will be recomputed on the fly when restarting the system

but before we go further let's think what are acces cases :


<index | timestamp  ||| count > - acces:

searching all indexes up to given timestamp for merge.


searching all indexes up to given timestamp for insert new item.

inserting new timestamps

delete old timestamp

so read is always sequential

hmm we can use in  memory only  structure, and log new timestamps to per table file,
then during compaction update file to new one

so if crash read log, and update persistent on save


<Key(Tuple) ||| Index> -> acces:

search : when we get tuple we can check whether it already has index by searching b+tree
insert : assign new index
search: find all matching tuples in other table


<Match ||| Key(Tuple)> -> acces:

search find all matching tuples in other table
insert, create new entry that will point to right tuple


------------------------------------------------------------------

all in all we could simpy store


<tuple ||| index ||| count > as persistent data.

+ log file for deltas

+ in memory <index| timestamp -> count > data structure for deltas

and build b+tree indexed by:


tuple for getting correct index'es

match field for gettign correct tuples for joins

Now there is also matter of indexes:

cause in theory we could also build third b+tree that uses indexes this should be light since indexes are just ints so
small data. However is it really needed? we could load it into memory once at the beginning and then once to persistent
storage at the end


So now we can implement api and then see if we can replace our storage with this design


also all our storage can be single threaded since we work on single node by one thread at once

*/

#ifndef ALICEDBSTORAGE
#define ALICEDBSTORAGE

#include "BufferPool.h"
#include "Common.h"
#include "TablePage.h"

#include <filesystem>
#include <fstream>
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
	template <typename Type>
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
			deltas_[idx].insert(d);
			return true;
		}
	}

	void Delete(const index idx) {
		deltas_.erase(idx);
	}

	/**
	 * @brief merge tuples by summing values, by index for given table up to end_timestamp
	 */
	// generic compact deltas work's for almost any kind of node (doesn't work for
	// aggregations we will see :) )
	void Merge(const timestamp end_ts) {
		for (auto &[idx, deltas] : deltas_) {
			int previous_count = 0;
			timestamp ts = 0;

			for (auto it = deltas.rbegin(); it != deltas.rend();) {
				previous_count += it->count;
				ts = it->ts;
				auto base_it = std::next(it).base();
				base_it = deltas.erase(base_it);
				it = std::reverse_iterator<decltype(base_it)>(base_it);

				// Check the condition: ts < ts_ - frontier_ts_

				// if current delta has bigger tiemstamp than one we are setting, or we
				// iterated all deltas insert accumulated delta and break loop
				if (it == deltas.rend() || it->ts > end_ts) {
					deltas.insert(Delta {ts, previous_count});
					break;
				} else {
					continue;
				}
			}
		}

		UpdateLogFile();
	}

	/**
	 * @brief returns multiset of all the deltas for given key
	 */
	inline std::multiset<Delta, DeltaComparator> &Scan(const index idx) {
		return this->deltas_[idx];
	}

	// return oldest delta for index
	inline Delta Oldest(index idx) {
		return *deltas_[idx].rbegin();
	}

	inline size_t Size() {
		return this->deltas_.size();
	}

private:
	void UpdateLogFile() {

		std::string tmp_filename = this->log_file_ + "_tmp";

		// open the temporary file for writing
		std::ofstream file_stream(tmp_filename, std::ios::out);

		for (auto &[idx, mst] : deltas_) {
			// write the index and the size of the multiset
			file_stream << idx << " " << mst.size() << "\n";

			// write out each Delta as "count ts"
			for (auto &dlt : mst) {
				file_stream << dlt.count << " " << dlt.ts;
			}
			file_stream << "\n";
		}

		// flush
		file_stream.close();

		// atomically replace old file
		std::filesystem::rename(tmp_filename, this->log_file_);
	}

	int ReadLogFile() {
		std::ifstream file_stream(this->log_file_, std::ios::in);
		// doesn't exists
		if (!file_stream) {
			return -1;
		}

		// clrear in mem stuff if there is any for some reason
		deltas_.clear();

		while (true) {
			index idx;
			std::size_t num_deltas;

			// Try to read <index> and <numDeltas>
			if (!(file_stream >> idx >> num_deltas)) {
				// eof
				return 0;
				break;
			}

			std::multiset<Delta, DeltaComparator> ms;

			// Read <count, ts> pairs 'numDeltas' times
			for (std::size_t i = 0; i < num_deltas; i++) {
				Delta d;
				// corrupted
				if (!(file_stream >> d.count >> d.ts)) {
					return 1;
				}
				ms.insert(d);
			}

			// move the collected deltas into the map
			deltas_[idx] = std::move(ms);
		}

		return 0;
	}

	// multiset of deltas for each index
	std::unordered_map<index, std::multiset<Delta, DeltaComparator>> deltas_;

	std::string log_file_;
};

/**
 *  @brief persistent storage
 *
 */

struct StorageIndex {
	index page_id_;
	index tuple_id_;
};

template <typename Key>
class BTree {

	BTree(BufferPool *bp);

	// return true if key was not present, else returns false, sets idx to corresponding index
	bool Insert(const Key &key, const StorageIndex &idx);

	bool Delete(Key *key);

	// searches for key if it finds it sets idx to corresponding index, and returns true,
	// else returns alse
	bool Search(const Key &key, const StorageIndex &idx);

private:
	std::vector<index> btree_page_indexes_;
};

template <typename Type>
struct HeapState{
	const Type* data_;
	index idx_;
};


template <typename Type>
class HeapIterator {
public:
	HeapIterator(index *page_idx, index tpl_idx, BufferPool *bp, unsigned int tuples_per_page, unsigned int pages_count)
	    : page_idx_ {page_idx}, tpl_idx_ {tpl_idx}, bp_ {bp}, tuples_per_page_ {tuples_per_page}, pages_count_{pages_count}, start_page_idx_{page_idx} {
	}

	HeapState<Type> Get() {
		this->LoadPage();	
		return HeapState<Type>(current_page_->Get(tpl_idx_), this->tuples_per_page_ *  *page_idx_ + tpl_idx_); 
	}

	HeapIterator &operator++() {

		this->LoadPage();	
		
		
		this->tpl_idx_++;
		
		while(!this->current_page_->Contains(tpl_idx_)){
			/** @todo  we need to make sure we didn't reach end of pages, and if so return heapiterator end or sth */
			
			if (this->tpl_idx_ == this->tuples_per_page_) {
				this->tpl_idx_ = 0;
				this->page_idx_++;
				// we reached end, thus we won't find tuple and must return
				if(this->page_idx_ == this-> start_page_idx_ + this->pages_count_){
					return *this;
				}
			}
			else{
				this->tpl_idx_++;
			}	
		}
		
		return *this;


	} // Prefix increment

	void LoadPage(){
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

	unsigned int pages_count_;
	index *start_page_idx_;

	std::unique_ptr<TablePageReadOnly<Type>> current_page_;
};

/**
 * @brief all the stuff that might be needed,
 * B+tree's? we got em,
 * Delta's with persistent storage? you guessed it we got em too
 */
template <typename Type>
class Table {
public:
	Table(std::string delta_storage_fname, std::vector<index> &data_page_indexes, std::vector<index> &btree_indexes,
	      BufferPool *bp, Graph *g)
	    : bp_ {bp}, g_ {g}, ds_ {std::make_unique<DeltaStorage>(delta_storage_fname)},
	      data_page_indexes_ {data_page_indexes}, btree_indexes_ {btree_indexes},
	      tuples_per_page_ {PageSize / (sizeof(bool) + sizeof(Type))} {
	}

	
	// return index if data already present in table, doesn't insert but just return index
	index Insert(const Type &in_data) {
		// firt insert into page, if already contains doesn't need to insert
		// this will return index,
		// then call insert on b+tree with in_data(key), index(leaf) for btreetable
		// then call insert on b+tree(match one) with in_data(key), index(leaf) for matchbtreetable

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
		} 
			
			write_page = std::make_unique<TablePage<Type>>(this->bp_, this->data_page_indexes_[this->current_page_idx_],
			                                               this->tuples_per_page_);
			
			// iterate to next pages to check if they have free space
			while(!write_page->Insert(in_data, &idx) && this->current_page_idx_ < this->data_page_indexes_.size()){
				this->current_page_idx_++;
				write_page = std::make_unique<TablePage<Type>>(this->bp_, this->data_page_indexes_[this->current_page_idx_],
			                                               	this->tuples_per_page_);
				if(write_page->Insert(in_data, &idx)){
					return idx + this->tuples_per_page_ * this->current_page_idx_;
				}
			}
			
			// if there is no place left in current write page, allocate new one
			this->current_page_idx_++;
			write_page = std::make_unique<TablePage<Type>>(this->bp_, this->tuples_per_page_);
			this->data_page_indexes_.push_back(write_page->GetDiskIndex());
			write_page->Insert(in_data, &idx);

		return idx + this->tuples_per_page_ * this->current_page_idx_;
	}

	// searches for data(key) in table using (btree/ heap search ) if finds returns true and sets index value to found
	// index
	bool Search(const Type &data, index *idx) {

		for (HeapIterator<Type> it = this->begin(); it != this->end(); ++it) {
			HeapState<Type> state = it.Get();
			if (std::memcmp(&data, state.data_ , sizeof(Type)) == 0) {
				*idx = state.idx_;
				return true;
			}
		}

		return false;
	}

	/**
	 * @brief deletes all tuples that are older than ts,
	 * if zeros_only is set only those for which current delta count is zero are deleted
	 *  
	 * @return list of deleted tuple indexes
	 * */
	std::vector<index> GarbageCollect(timestamp ts, bool zeros_only){
		// first we got to iterate delta storage to get vector of all the indexes we wish to delete
		std::vector<index> delete_indexes;

		for(auto it = this->ds_->deltas_.begin(); it != this->ds_->deltas_.end(); it++){

			if(it->second.rbegin()->ts < ts){

				if(zeros_only)
				{
					// calculate count and check if it's zero, then we can delete
					int sum = 0;
					for(const auto &dlt : it->second){
						sum += dlt.count;
					}
					if(sum != 0){
						continue;
					}
				}
				// ok since we are here we don't care about count, or cout is actually zero, so we can delete
				delete_indexes.emplace_back(it->first);
				it = this->ds_->deltas_.erase(it);


			}

		}


		// then when we get the indexes we should iterate pages and mark those tuples as deleted

		std::sort(delete_indexes.begin(), delete_indexes.end());
		std::unique_ptr<TablePage<Type>> wip_page_;
		for(const auto &idx: delete_indexes){
			auto [page_idx, tpl_idx] =  this->IndexToStorageIndex(idx);
			if(!wip_page_ || wip_page_->GetDiskIndex() != page_idx){
				wip_page_ =  std::make_unique<TablePage<Type>>(this->bp_, page_idx,this->tuples_per_page_);
			}
			wip_page_->Remove(tpl_idx);
		}

		// update new insert page
		if(delete_indexes.size() > 0){
			auto [page_idx, _] =  this->IndexToStorageIndex(delete_indexes[0]);
			this->current_page_idx_ = page_idx;
		}
		
		// then we can return list of deleted indexes, then we set current write page to first one that has holes
		return delete_indexes;

		
		// and also when we will insert new pages we will now consider this page, and then next ones.. till we reach end, only then we will alloc new page

		//  alternative

		// move tuples from newest pages, such that those holes are instantly filled, then we need to return not only list of deletes, but also list of updates,
		// so it seems like more work, but i guess less fragmentation? but wait is it really bad? eventually those holes will get filled
	
		// ok first option makes more sense for now so we will use it :)



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

	// other will be iterate all tuples, so heap based for
	// cross join and compact delta for distinct node

	/** @todo all iterators should return tuple or sth so like data - pointer | index */
	HeapIterator<Type> begin() {
		return HeapIterator<Type>(this->data_page_indexes_.data(), 0, this->bp_, this->tuples_per_page_, this->data_page_indexes_.size());
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
	std::multiset<Delta, DeltaComparator> &Scan(const index idx) {
		return this->ds_->deltas_[idx];
	}

	// return oldest delta for index
	inline Delta OldestDelta(index idx) {
		return this->ds_->Oldest(idx);
	}

	inline size_t DeltasSize() {
		return this->ds_->Size();
	}

private:
	inline index StorageIndexToIndex(StorageIndex sidx) {
		return this->tuples_per_page * sidx.page_id_ + sidx.tuple_id_;
	}

	StorageIndex IndexToStorageIndex(index idx) {
		return {idx / this->tuples_per_page_, idx % this->tuples_per_page_};
	}

	// methods for page accesing etc

	// heap data pages
	unsigned int tuples_per_page_;

	std::vector<index> &data_page_indexes_;
	index current_page_idx_ = 0;

	std::vector<index> &btree_indexes_;

	// delta storage
	std::unique_ptr<DeltaStorage> ds_;

	// buffer pool pointer
	BufferPool *bp_;

	// graph pointer
	Graph *g_;
};

} // namespace AliceDB
#endif