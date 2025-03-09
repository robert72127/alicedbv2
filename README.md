Alicedb is incremental database library for C++, that works by executing queries in streaming model.

## Development Commands

### clang-format: 
#### check
python3 scripts/run-clang-format.py -r --exclude src/third_party --exclude tests src include
### apply
python3 scripts/run-clang-format.py -r --exclude src/third_party --exclude tests src include --in-place 

### clang-tidy:
#### check
python3 scripts/run-clang-tidy.py -p build

### apply
python3 scripts/run-clang-tidy.py -p build -fix

#### cmake build debug:
cmake --buid build
cmake -S. -Bbuild -DCMAKE_BUILD_TYPE=Debug

##### cmake build release:
cmake --buid build
cmake -S. -Bbuild -DCMAKE_BUILD_TYPE=Release

### Usage

How it works?

first we have to create database instance, and assign numbers of worker thread we want it to use

```
auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);
```

then we can create DAGs that will represent our query

```
auto *view = g->View(
    g->Join(
        [](const Person &p)  { return p.favourite_dog_race;},
        [](const Dog &d)  { return d.name;},
        [](const Person &p, const Dog &d) { 
            return  JoinDogPerson{
                      .name=p.name,
                      .surname=p.surname,
                      .favourite_dog_race=d.name,
                      .dog_cost=d.cost,
                      .account_balace=p.account_balance,
                      .age=p.age
                    };
          },
          g->Filter(
              [](const Person &p) -> bool {return p.age > 18;},
              g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
          ),
          g->Source<Dog>(AliceDB::ProducerType::FILE, dogs_fname, parseDog,0)
      )                    
);
```

and finally we can start processing data by calling

```
db->StartGraph(g);
```

and stop it, using 
```
db->StopGraph(g);
```


### How it works:

#### Graph layer:

Graph layer is responsible for keeping track of nodes, monitioring whether it's already running, scheduling node's computations 
and for type inference. 


#### WorkerPool:

WorkerPool manages worker threads and assign works to them, by scheduling computations for all nodes from all graphs.

#### Storage:

##### Table

Main abstraction for accesing persistent data by Nodes,
It's stores deltas, datastructure for efficient search of keys and tuples accesing.
Is responsible for compressing deltas and garbage collection.
Supports standard operations of insert delete search .

##### Buffer Pool

Integrated with disk manager through preregistered buffers, used to load disk pages into memory


##### Disk Manager

Disk manager is responsible for disk I/O
it has it's own single worker that is responsible for managing R/W requests with IO_URING

Work operations will return future that will be satisfied when actual operation
is performed

disk manager, tries to perform disk operations in batch

#### Producers

Ingest and parse data into the system, for now limited to reading from file and from tcp server.

#### Nodes

##### Processing node in streaming database graph

for each tuple we want to be able to:
1) store it in persistent storage, with count of such tuples at each point of time.
2) Be able to effectively retrive it
3) Be able to decide whether this tuple can be deleted from the system


 4) Join and group by will also need a way to effectively search by only part of tuple, ie by specific fields,
 defined by transformation from original tuple.


 Each Table will maintain it's own indexes.
 
There are following types of nodes:
 
###### Projection, 

```
    g->Projection(
        [](const InType &t) { return OutType {.field_x=t.field_z}; },
        g->Source<InType>(Producer , in_file, parse_function,0)
    )
```

Stateless node that projects data from one struct to another

###### Filter, 

here is example combining it with projection

```
    g->Projection(
        [](const InType &t) { return OutType {.field_x=t.field_z}; },
        g->Filter(
            [](const InType &t) -> bool {return t.field_x > 18 && t.field_y[0] == 'S' ;},
            g->Source<Type>(Producer , in_file, parse_function,0)
        )
    )
```

Stateless node, that filters tuples based on user provided filter function.

###### Union, Except

Since those work in almost identical way (only difference being adding/subtracting) we will only provide example of Union

```
    g->Union(
        g->Projection(
            [](const InType_1 &t) { return OutType_1 {.field_x=t.field_x, .field_y=t.field_y}; },
            g->Source<InType>(Producer , in_file_1, parse_function_1,0)
        ),
        g->Projection(
            [](const InType_2 &t) { return OutType_2 {.field_a=t.field_a, .field_b=t.field.b}; },
            g->Source<InType>(Producer , in_file_2, parse_function_2,0)
        )
    )
```

both union and except are actually implemented as stateless nodes in Node.h,
Graph.h automatically sets Distinct node as output of them so that state is persisted
and we correctly know when to emit insert and delete to out node, for each tuple.

###### Intersect
 
``` 
    g->Intersect(
        g->Projection(
            [](const InType_1 &t) { return OutType_1 {.field_x=t.field_x, .field_y=t.field_y}; },
            g->Source<InType>(Producer , in_file_1, parse_function_1,0)
        ),
        g->Projection(
            [](const InType_2 &t) { return OutType_2 {.field_a=t.field_a, .field_b=t.field.b}; },
            g->Source<InType>(Producer , in_file_2, parse_function_2,0)
        )
    )
```
Uses same mechanism as union/except where distinct node is automatically set as output.
It combines tuples from two tables, only passing those that appeard in both
 
Statefull node that stores two tables one for each source
count should be left_count x right_count  

###### Product
 
 ```
        g->View(
            g->CrossJoin(
                [](const Person &left, const Person &right){
                    return PairPeople{
                        .lname=left.name,
                        .lsurname=left.surname,
                        .lage=left.age,
                        .rname=right.name,
                        .rsurname=right.surname,
                        .rage=right.age
                    };
                },
                g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0),
                g->Source<Person>(AliceDB::ProducerType::FILE , people_fname2, parsePerson,0)
            )
        );

```
	
 It takes two tables as inputs and produce X product
 it needs to store two Tables
 and it doesn't need to know their fields
 
This statefull node matches every tuple with every other
 
###### Distinct

This node output only single outtuple for given in tuple, its responsible for making sure outnode has 1/0 count for given tuple

```
    g->Distinct(
        g->Source<InType>(Producer , in_file_1, parse_function_1,0)
    );
```

###### Join

 joins nodes from two tuples based on match field

```
    // joins dog and person on dog race and person's favourite dog race
     g->Join(
        [](const Person &p)  { return p.favourite_dog_race;},
        [](const Dog &d)  { return d.name;},
        [](const Person &p, const Dog &d) { 
            return  JoinDogPerson{
                .name=p.name,
                .surname=p.surname,
                .favourite_dog_race=d.name,
                .dog_cost=d.cost,
                .account_balace=p.account_balance,
                .age=p.age
            };
        },
        g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
        g->Source<Dog>(AliceDB::ProducerType::FILE, dogs_fname, parseDog,0)
    )
```

 
For this statefull node we need to keep both inputs in tables.


###### Aggregations, 

```
    // dummy example to explain idea,
    // let's sum accounts balance of all people based on their name

    auto *view = 
        g->View(
                g->AggregateBy(
                    [](const Person &p) { return Name{.name = p.name}; },
                    [](const Person &p, int count, const NamedBalance &nb, bool first ){
                        return NamedBalance{
                            .name = p.name,
                            .account_balance = first?  p.account_balance : p.account_balance + nb.account_balance
                        };
                    },
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
                )
        );
```

This statefull node stores single table
 

##### Consistency:
 
 After computation for each node we will update it's timestamp,
 after timestamp is updated on node marked as output
 it will trigger update of timestamps in all of it's input node's
 then those node's will know that if they hold multiple version they can compact
 older version with timestamp smaller than current
 
 
##### Update delete and insert operations:
 
 update is just delete and then insert one after another,
 
 delete is also insert but with negative value
 
##### Garbage collection

We can configure system to use three different garbage collecting polcies
    * Never delete tuples, store all data
    * Delete all tuples whose never version is older than current delete timestamp
    * Delete all tuples whose never version is older than current delete timestamp but whose delta count is 0

Configuration is done throught struct
```
struct GarbageCollectSettings {
	timestamp clean_freq_;
	timestamp delete_age_;
	bool use_garbage_collector;
	bool remove_zeros_only;
};
```

that can be passed to database instance
```
DataBase(std::filesystem::path database_directory, unsigned int worker_threads_count = 1, GarbageCollectSettings *gb_settings = nullptr)
```

##### Node's api:

  all nodes implement following api:

  Compute: this will perform all computable action <for now> that is:
 
  CleanCache: - keeps track on nuber of output nodes, to know when all of them processed this node's output cacne and it can be cleaned 
  
  UpdateTimestamp - Updating timestamp of internal tables, compacting deltas, inserting tuples into persistent storage, providing output into out_cache
 
  AddOutNode - used by graph layer to keep track of all out nodes of given node, used for timestamp update

  OldestTsToKeep - for all out nodes ts, returns smallest one (oldest ts) this is oldest time for which given node holds tuple noncompacted versions

  Output: return out_cache of given node: this will be used by graph layer for chaining nodes


  GetFrontierTs: - returns how much time in the past from current time do we have to store deltas

  GetTs - returns current time of node

### Future Extensions:

#### SQL Layer:

Our library is implemented in such way that it would allow for accepting sql query as an input, and then generating AliceDB code
it would generate structs, and recursively compute graph layout, by chaining nodes defined in previous sections.
Then such code could be compiled as shared lib and dynamically linked to current process.

(cmu 15-721)[https://15721.courses.cs.cmu.edu/spring2024/slides/07-compilation.pdf]

#### Sources

(unixism)[https://unixism.net/loti/tutorial/fixed_buffers.html] - fixed buffers

https://www.vldb.org/pvldb/vol7/p853-klonatos.pdf dbsp - as general for processing framework

https://www.skyzh.dev/blog/2023-12-28-store-of-streaming-states/ - overwiew of streaming systems

[dida why] https://github.com/jamii/dida/blob/main/docs/why.md - dida simple streaming database based on differential dataflow,
this document explain needs for internal consistency, eventual consistency. And batch processing in streaming system.


