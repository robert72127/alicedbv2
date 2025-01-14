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

#include "Common.h"

namespace AliceDB{

class DeltaStorage{
public:

    bool Insert(std::string table_name, int index, const Delta &d );

    /**
     * @brief merge tuples by summing values, by index for given table up to end_timestamp
     */
    bool Merge(std::string table_name, timestamp end_ts);

    /**
     * @brief returns vector of all the deltas for given key up to timestamp : up_to_ts
     */
    std::vector<Delta> Scan(std::string table_name, index i, timestamp up_to_ts);


    /**
     * @brief remove all key|value pairs associated with given table
     */
    bool DeleteTable(std::string table_name);




private:

    std::vector<std::string> tables_;


};

}

#endif