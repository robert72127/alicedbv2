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

first we have to create graph instance, and assign numbers of worker thread we want it to use


auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

then we can create DAG instances that will represent our query

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

and finally we can start processing data by calling

db->StartGraph(g);

and stop it, using 

db->StopGraph(g);


after we created source we can create our queries that will be executed in streaming way

g.call("Select name, surname, accout_balance from S where accout_balance > 1500
    join S2 on accout_balance > dog_price as S3
 )

and now we can either show results from S3, treat it as new source for new views, or write it somewhere else (not included for now)



### How it works:

#### Graph layer:

Graph layer is responsible for keeping track of nodes, monitioring whether it's already running, scheduling node's computations 
and for type inference. 


#### WorkerPool:

WorkerPool dynamically allocate new threads, based on amount of currently running graphs, and perform computations on them.

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
it has it's own single thread that is responsible for
Compaction, and managing requests with IO_URING

Work operationss will return future that will be satisfied when actual operation
is performed

disk manager, delay disk write, perform in batch when at least some critical %
of data  set write flag

when to write heuristics ?

 if there is read request also use this occasion to perform write on all those
that want it

file descriptor and
all pages of buffer pool are  registered for io_uring
/

Storage layer utilises async disk worker & io_uring preregistered with buffer pool, using single file for storage

#### Producers

Ingest and parse data into the system, for now limited to reading from file and from tcp server.


#### Nodes

##### Processing node in streaming database graph

for each tuple we would want to:
1) store it in persistent storage, with count of such tuples in each point of time.
2) Be able to effectively retrive it
3) Be able to decide whether this tuple can be deleted from the system


 4) Join and group by will also need a way to effectively search by only part of tuple, ie by specific fields,
 defined by transformation from original tuple 


 Each Table will maintain it's own indexes
 

Node can be of type:
 
###### Projection, 

g->Projection(
    [](const InType &t) { return OutType {.field_x=t.field_z}; },
    g->Source<InType>(Producer , in_file, parse_function,0)
)

doesn't care about state, doesn't care about count, doesn't care about index or timestamps, just compuuuutes on data field

###### Filter, 

here is example combining it with projection

g->Projection(
    [](const InType &t) { return OutType {.field_x=t.field_z}; },
    g->Filter(
        [](const InType &t) -> bool {return t.field_x > 18 && t.field_y[0] == 'S' ;},
        g->Source<Type>(Producer , in_file, parse_function,0)
    )
)

which corresponds to sql

select field_z from InType
where field_x > 18, and field_y[0] equals S

 
It's selection with WHERE

doesn't care about state, doesn't care about count, doesn't care about index or timestamps, just compuuuutes on data field

###### Union, Except

since those works in almost identical way we will only provide example of Union

we can think of normal sql union as

 SELECT column1, column2, ...
   FROM table1
   UNION
   SELECT column1, column2, ...
   FROM table2;


wheres in ou case we would first do projection on table1 and table2 and the use union node,

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

both union and except are actually implemented as stateless nodes in Node.h,
Graph.h automatically sets Distinct node as output of them so that state is persisted
and we correctly know when to emit insert and delete to out node

 
###### Intersect which is sql substitute of
 
 SELECT column1, column2, ...
  FROM table1
 INTERSECT
 SELECT column1, column2, ...
   FROM table2;
 
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

Works very similiar to defined above, also places distinct node to track when to emit insert delete etc.
But intersect operator itself is also stateful, since it need other table state to compute results


 It combines tuples from two tables, only passing those that appeard in both
 
 keep two tables one for each source
 
 count should be left_count x right_count 
 

###### Product
 
g->CrossJoin(
                [](const Person &left, const Person &right){
                    return CrossPerson{
                        .lname=left.name,
                        .lsurname=left.surname,
                        .lage=left.age,
                        .rname=right.name,
                        .rsurname=right.surname,
                        .rage=right.age
                    };
                },
                g->Source(prod_1, 0),
                g->Source(prod_2,0)
            )
 // this is projection
 SELECT Employees.ID, Employees.Name, Employees.Department,
      Projects.Project_ID, Projects.Project_Name
   FROM Employees
 
   CROSS JOIN Projects
 	
 It takes two tables as ingerses and produce X product
 it needs to store two Tables
 and it doesn't need to know their fields
 
it matches every tuple with every other

count will be multiplied of first and second
 
###### Distinct

This node output only single outtuple for given in tuple, its responsible for keeping track on when tuple count becomes positive/negative

###### Join
 
 SELECT Employees.Name, Departments.Dept_Name
   FROM Employees
 JOIN Departments
 ON Employees.Department = Departments.Dept_ID;
 
 WE need to keep both inputs in tables
 
 
 count will be multiplication of first and second
 
###### Aggregations,  but let's include only those with group by: Count, sum avg, min, max are typical
 
 
 SELECT customer_id, COUNT() AS order_count, SUM(amount) AS total_spent
 FROM orders
 GROUP BY customer_id;
 The GROUP BY clause groups rows that have the same values in specified columns and applies the aggregation to each group. 
 
 SELECT customer_id, SUM(amount) AS total_spent
 FROM orders
 GROUP BY customer_id
 
 HAVING filters groups based on aggregated values, while WHERE filters rows before aggregation.
 
 ok so in general it's stateful single in cache single out_cache, that appiles function to given rows,
 
 it will create tuple with data | group_by field | rest of fields with appiled function
 
 but the catch is we will need to search etc. tuples only by aggregate fields


This design allows the graph to handle inserts and deletes in a streaming fashion, updating stateful nodes and propagating changes downstream in real time.

##### Consistency:
 
 After computation for each node we will update it's timestamp,
 after timestamp is updated on node marked as output
 it will trigger update second update of timestamps in all of it's input node's
 then those node's will know that if they hold multiple version they can delete
 older one's with timestamp smaller than current
 
 
##### Update delete and insert:
 
 update is just delete and then insert one after another,
 
 delete is also insert but with negative value, negative values won't be presented when printing output
 
 
##### Garbage collection
 
 We will iterate all pages of table & delete old tuples and compact pages
 during this iteration we will need to hold some structure that will tell us
 if we already seen given tuple, and for all timestamp less than predefined remove those
 
 
##### Node's api:

  all nodes implement following api

  Compute: this will perform all computable action <for now> that is:
 
  Updating timestamp of internal tables, compacting deltas, inserting tuples into persistent storage, providing output into out_cache
 
   Output: return out_cache of given node: this will be used by graph layer for chaining nodes
 
   UpdateTimestamp: this will be called by output nodes it will update ts and propagate to input nodes
   updating table state is more costly and will be handled by worker threads


  CleanCache: - keeps track on nuber of output nodes, to know when all of them processed this node's output cacne and it can be cleaned 


	GetFrontierTs: - returns how much time in the past from current time do we have to store deltas


### Future Extensions:

#### SQL Layer:

SQL queries can be compiled into our C++ graph program 
then this program would be compiled using off the shelf C++ compiler such as GCC and linked with currently running process,
note our generated code wouldn't contain code for general DataStorage. Instead it would just generate structs, and recursively compute 
graph layout as defined above.

(cmu 15-721)[https://15721.courses.cs.cmu.edu/spring2024/slides/07-compilation.pdf]



#### Sources


(unixism)[https://unixism.net/loti/tutorial/fixed_buffers.html] - fixed buffers

https://www.vldb.org/pvldb/vol7/p853-klonatos.pdf dbsp - as general for processing framework

https://www.skyzh.dev/blog/2023-12-28-store-of-streaming-states/ - overwiew of streaming systems

[dida why] https://github.com/jamii/dida/blob/main/docs/why.md - dida simple streaming database based on differential dataflow,
this document explain needs for internal consistency, eventual consistency. And batch processing in streaming system.


