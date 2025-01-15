/*
We need to store three things:

< Index | Timestamp > -> Count

we will: 

insert new counts for given index and timestamp
search for all <index|timestamp , count> tuples up to some timestamp

*********************************************************
seems like something akin to lsm tree will work best here, like rocksdb, deffinetely we need sorted approach tho

and as a bonus we can actually store single global rockdb table for all tables, we will just prefix with table name
----------------------------------------------------------------------

<Key (large), index> 
we will:

insert new tuple
for given key retrieve index associated with it

this could be just b+tree or hash table with straighforward lookups

-----------------------------------------------------------------------

Match | Key mapping

used when:

we have two tables, first uses f1 for match and second uses f2, and we want to do things such as join

then for new tuples from table 1 we compute f1(tuple) = match1 and then compute f2(tuple) for each tuple in table 2 
and perform join on matching from both

how to store: as secondary index? 
    by multiple fields?
    by new match field?

as separate table?
    with mapping <match | key> -> this feels better cause it allows us to store even more interesting structures,
    and potentiall treat key as single field instead of splitting it into separate columns

so it also will be b+tree / hash_table

*/

#ifndef ALICEDBSTORAGE
#define ALICEDBSTORAGE

#include <string>
#include <vector>
#include <map>
#include <set>

#include "Common.h"

namespace AliceDB{


/**
 * @brief for now this is just wrapper around stl structures, but putting this into separate class will make implementing
 * real storage easier
 * 
 * later we will switch to using single table rockdb with table_name_prefix to separate deltas from different tables
 */
class DeltaStorage{
public:
    /** 
     * @brief insert new delta into the table
     */
    bool Insert(std::string table_name, index idx, const Delta &d ){        
        // get correct table, and if it doesn't exists, create it
        if(!this->tables_.contains(table_name)){
            this->tables_[table_name] = std::vector<std::multiset<Delta, DeltaComparator>>();
        }
        std::vector<std::multiset<Delta, DeltaComparator>> & index_to_deltas_ref = this->tables_[table_name];

        // get correct index
        size_t itd_size = index_to_deltas_ref.size();
        while(idx >= itd_size ){
            index_to_deltas_ref.push_back({});
        }
        
        std::multiset<Delta, DeltaComparator> & deltas_ref = index_to_deltas_ref[idx];
        deltas_ref.insert(d);
    }

    /**
     * @brief merge tuples by summing values, by index for given table up to end_timestamp
     */
    bool Merge(std::string table_name, timestamp end_ts){
        if(!this->tables_.contains(table_name)){
            return false;
        }
        std::vector<std::multiset<Delta, DeltaComparator>> &table = this->tables_[table_name];

        for (int index = 0; index <  table.size(); index++) {
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
    std::multiset<Delta, DeltaComparator> Scan(std::string table_name, index idx){
        if(! this->tables_.contains(table_name)){
            return {};
        }

        std::vector<std::multiset<Delta, DeltaComparator>> &table = this->tables_[table_name];
        if(table.size() <= idx){
            return {};
        }

        return table[idx];
    }

    /**
     * @brief remove all key|value pairs associated with given table
     */
    bool DeleteTable(std::string table_name){
        this->tables_.erase(table_name);
    }

private:

    /** @brief table is accesed by string(name) and it holds vector of multisets of deltast
     * 
     * string -> return vector of multisets of deltas
     * int -> return multiset of deltas for given index
     * multiset of deltas -> deltas sorted by timestamp
     */
    std::map <std::string, std::vector<std::multiset<Delta, DeltaComparator>> > tables_;
};

}

/**
 * @brief 
 * Storage for normal: Key|Value mappings, that is for:
 *  Key|index mapping -> this is always one to one
 *  and for 
 *  MatchField | Key -> this is many to one
 * 
 *  best way would be to use some standard storage technology with buffer pool and b-tree | hashtable.
 * 
 *  What api do we need?
 *  We might want to switch to iterator api, but for now let's use this simple one.
 *  We will use normal bufferpool with disk backed storage
 * 
 *
 * 
 *  What api do we need for 
 * 
 *  for match-> tuple -> we need b-tree based search, and normal insertion with possible duplicated match keys
 * 
 *  for tuple -> index we need b-tree based search,heap search and normal insertion 
 *  */

template <typename K, typename V>
class Table{

    KVStorage(std::string table_name);

    /** @brief returns vector of values associated with given key */
    std::vector<V> Get(K key);
    
    /** @brief inserts new key value pair into the table */
    bool Put(K key, V value);

};

#endif