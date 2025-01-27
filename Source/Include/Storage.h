/*
We need to store three things:

< Index | Timestamp > -> Count - this can either be done entirely in memory or also using btree, cause it gives us prefix search for free

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

cause in theory we could also build third b+tree that uses indexes this should be light since indexes are just ints so small data.
However is it really needed? we could load it into memory once at the beginning and then once to persistent storage at the end


So now we can implement api and then see if we can replace our storage with this design


also all our storage can be single threaded since we work on single node by one thread at once

*/

#ifndef ALICEDBSTORAGE
#define ALICEDBSTORAGE

#include <map>
#include <set>
#include <string>
#include <vector>

#include "Common.h"

namespace AliceDB {

/**
 * @brief for now this is just wrapper around stl structures, but putting this into separate
 * class will make implementing real storage easier
 *
 * later we will switch to using single table rockdb with table_name_prefix to separate deltas
 * from different tables
 */
class DeltaStorage {
 public:
  /**
   * @brief insert new delta into the table
   */
  bool Insert(std::string table_name, index idx, const Delta &d) {
    // get correct table, and if it doesn't exists, create it
    if (!this->tables_.contains(table_name)) {
      this->tables_[table_name] = std::vector<std::multiset<Delta, DeltaComparator>>();
    }
    std::vector<std::multiset<Delta, DeltaComparator>> &index_to_deltas_ref =
        this->tables_[table_name];

    // get correct index
    size_t itd_size = index_to_deltas_ref.size();
    while (idx >= itd_size) {
      index_to_deltas_ref.push_back({});
    }

    std::multiset<Delta, DeltaComparator> &deltas_ref = index_to_deltas_ref[idx];
    deltas_ref.insert(d);
  }

  /**
   * @brief merge tuples by summing values, by index for given table up to end_timestamp
   */
  bool Merge(std::string table_name, timestamp end_ts) {
    if (!this->tables_.contains(table_name)) {
      return false;
    }
    std::vector<std::multiset<Delta, DeltaComparator>> &table = this->tables_[table_name];

    for (int index = 0; index < table.size(); index++) {
      auto &deltas = table[index];
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
          deltas.insert(Delta{ts, previous_count});
          break;
        } else {
          continue;
        }
      }
    }
  }

  /**
   * @brief returns multiset of all the deltas for given key up to timestamp : up_to_ts
   */
  std::multiset<Delta, DeltaComparator> Scan(std::string table_name, index idx) {
    if (!this->tables_.contains(table_name)) {
      return {};
    }

    std::vector<std::multiset<Delta, DeltaComparator>> &table = this->tables_[table_name];
    if (table.size() <= idx) {
      return {};
    }

    return table[idx];
  }

  /**
   * @brief remove all key|value pairs associated with given table
   */
  bool DeleteTable(std::string table_name) { this->tables_.erase(table_name); }

 private:
  /** @brief table is accesed by string(name) and it holds vector of multisets of deltast
   *
   * string -> return vector of multisets of deltas
   * int -> return multiset of deltas for given index
   * multiset of deltas -> deltas sorted by timestamp
   */
  std::map<std::string, std::vector<std::multiset<Delta, DeltaComparator>>> tables_;
};

}  // namespace AliceDB

/**
 * @brief
 * Storage for normal: Key|Value mappings, that is for:
 *  Key|index mapping -> this is always one to one
 *  and for
 *  MatchField | Key -> this is many to one
 *
 *  best way would be to use some standard storage technology with buffer pool and b-tree |
 * hashtable.
 *
 *  What api do we need?
 *  We might want to switch to iterator api, but for now let's use this simple one.
 *  We will use normal bufferpool with disk backed storage
 *
 *
 *
 *  What api do we need for
 *
 *  for match-> tuple -> we need b-tree based search, and normal insertion with possible
 * duplicated match keys
 *
 *  for tuple -> index we need b-tree based search,heap search and normal insertion
 *  */

template <typename K, typename V>
class Table {
  KVStorage(std::string table_name);

  /** @brief returns vector of values associated with given key */
  std::vector<V> Get(K key);

  /** @brief inserts new key value pair into the table */
  bool Put(K key, V value);
};

#endif