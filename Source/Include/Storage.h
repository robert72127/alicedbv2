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
 */
class DeltaStorage{
public:
    /** 
     * @brief insert new delta into the table
     */
    void Insert(std::string table_name, index idx, const Delta &d ){        
        // get correct table, and if it doesn't exists, create it
        if(!this->tables_.contains(table_name)){
            this->tables_[table_name] = std::vector<std::multiset<Delta, DeltaComparator>>();
            this->table_next_index_[table_name] = 0;     
        }
        std::vector<std::multiset<Delta, DeltaComparator>> & index_to_deltas_ref = this->tables_[table_name];

        // get correct index
        size_t itd_size = index_to_deltas_ref.size();
        while(idx >= itd_size ){
            index_to_deltas_ref.push_back({});
            this->table_next_index_[table_name]++;
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
        this->table_next_index_.erase(table_name);
    }

private:

    /** @brief table is accesed by string(name) and it holds vector of multisets of deltast
     * 
     * string -> return vector of multisets of deltas
     * int -> return multiset of deltas for given index
     * multiset of deltas -> deltas sorted by timestamp
     */
    std::map <std::string, std::vector<std::multiset<Delta, DeltaComparator>> > tables_;
    // with what index should next value be inserted
    std::map<std::string, int> table_next_index_;


};

}

#endif