/*
We need to store three things:

< Index | Timestamp > -> Count - this can either be done entirely in memory or also using btree, cause it gives us
prefix search for free

<Key(Tuple) | index> - this can be done by btree

<Match | Keys> - hmm this can be probably done by creating btree

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

#include "Common.h"

#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace AliceDB {

/**
 * Storage for <index | delta > mappings
 * main idea is to store structure as stl container in memory and later overwrite log file on disk on compression op
 *
 */
class DeltaStorage {
public:
	friend Table;
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
	 */
	bool Insert(const index idx, const Delta &d) {
		// get correct index
		if (!this->deltas_.contains(idx)) {
			this->deltas_[idx] = {d};
		} else {
			deltas_[idx].insert(d);
		}
	}

	/**
	 * @brief merge tuples by summing values, by index for given table up to end_timestamp
	 */
	bool Merge(const timestamp end_ts) {
		for (int index = 0; index < deltas_.size(); index++) {
			auto &deltas = deltas_[index];
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

private:
	bool UpdateLogFile() {

		std::string tmp_filename = this->log_file_ + "_tmp";

		// open the temporary file for writing
		std::ofstream file_stream(tmp_filename, std::ios::out);

		for (auto &[idx, mst] : deltas_) {
			// write the index and the size of the multiset
			file_stream << idx << " " << mst.size();

			// write out each Delta as "count ts"
			for (auto &dlt : mst) {
				file_stream << " " << dlt.count << " " << dlt.ts;
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
			return;
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
	}

	// multiset of deltas for each index
	std::unordered_map<index, std::multiset<Delta, DeltaComparator>> deltas_;

	std::string log_file_;
};

class BufferPool;
/**
 *  @brief persistent storage
 *
 */
template <typename Key, typename Value>
class BTree {

	Btree(BufferPool *bp)

	    bool Insert(Key *k, Value *v);

	/** we should probably return vector of positions <page_index, tuple_index> so we will be able to access it in nice
	 * sorted order */
	std::vector<TablePosition> Search(Key *k);

	bool Delete(Key *k);

	private:
	std::vector<index> btree_page_indexes_;
};

/**
 * @brief all the stuff that might be needed,
 * B+tree's? we got em,
 * Delta's with persistent storage? you guessed it we got em too
 */

template <typename Type>
class Table {

	Table(std::string metadata_file, std::string delta_storage_fname, 
			std::vector<index> data_page_indexes, 
			BufferPool *bp): bp_{bp}, ds_{std::make_unique<DeltaStorage>(delta_storage_fname)}, data_page_indexes_{data_page_indexes} 
	{
	}

	void Insert(const char *in_data){}

	void Delete(const char *data){}


	// other will be iterate all tuples, so heap based for 
	// cross join
	class HeapIterator;
	HeapIterator begin();
	HeapIterator end();

	// methods to work with deltas
	bool InsertDelta(const index idx, const Delta &d){
		return this->ds_->Insert(idx, d);
	}
	bool MergeDelta(const timestamp end_ts) {
		return this->ds_->Merge( end_ts);
	}
	std::multiset<Delta, DeltaComparator> &Scan(const index idx) {
		return this->ds_->deltas_[idx];
	}


protected:
	std::vector<index> data_page_indexes_;

	std::unique_ptr<DeltaStorage> ds_;
	BufferPool *bp_;

};


template <typename Type>
class BtreeTable : public Table{

	BtreeTable(std::string metadata_file, std::string delta_storage_fname, 
			std::vector<index> data_page_indexes, 
			std::vector<index> btree_indexes, 
			BufferPool *bp): Table(metadata_file, delta_storage_fname, data_page_indexes, bp), btree_indexes_(btree_indexes) 
	{
	}

	~BtreeTable();

	/** @todo this should maybe work like iterator? ie return next tuple?
	 */
	class BtreeItertor;
	BtreeItertor begin(const char *find_data);
	BtreeItertor end();

protected:
	std::vector<index> btree_indexes_;
};


template <typename Type, typename MatchType>
class MatchTable: public : BtreeTable{

	MatchTable(std::string metadata_file, std::string delta_storage_fname, 
			std::vector<index> data_page_indexes, 
			std::vector<index> btree_indexes, 
			std::vector<index> match_indexes, 
			BufferPool *bp): BtreeTable(metadata_file, delta_storage_fname, data_page_indexes, bp, btree_indexes), match_indexes_(match_indexes) {}

	~MatchTable(); 
	
	// this is only needed for join node
	class MatchIterator;
	
	MatchIterator<MatchTpe> begin(const char *find_data);
	MatchIterator<MatchTpe> end();

private:
	std::vector<index> match_indexes_;
};


} // namespace AliceDB
#endif