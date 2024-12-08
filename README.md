![Alice](./assets/logo.png)
Alicedb is database managment system, that works by transpiling queries into C++, and executing them in streaming model.

### Usage

How it works?

first we have to create graph instance, and assign numebrs of worker thread we want it to use

auto g = std::make_shared<Graph>(worker_count);

then we need to create some source nodes, design will allow to create many different kinds of source nodes but for the demo,
we will only provide file as a source

g.call("Create source S from File filename
    column1 datatype1
    ....
    "
)

g.call("Create source S2 from File filename2
    column1 datatype1
    ....
"
)

after we created source we can create our queries that will be executed in streaming way

g.call("Select name, surname, accout_balance from S where accout_balance > 1500
    join S2 on accout_balance > dog_price as S3
 )

and now we can either show results from S3, treat it as new source for new views, or write it somewhere else (not included for now)


### How it works:

#### SQL Layer:

SQL query is compiled into C++ graph program that streamingly computes query results, 
then this program is compiled using off the shelf C++ compiler such as GCC and linked with currently running process,
note our generated code doesn't contain code for say DataStorage, instead it uses call's to Object that already exist in main process. we pass this object as a pointer to dynamically linked library

#### Graph layer:

Graph layer stores nodes in vector and stores they dependency graph, then uses set of workers to perform computations on this graph
calling compute on each node in topological order

#### Nodes

##### Processing node in streaming database graph
 
 for storage we need to:
 1) be able to find count of tuple corresponding to given tuple in queue, - we can check that from the latest value and current one from queue
 2) be able to tell if given value at time T can be deleted from persistent storage - we can do that by checking if it's newest value and if it's ts is greater or less than current one
 3) be able to search for all tuples that matches given one, yes by hashing on all fields, and storing mapping from this hash to list of indexes in table
 since collision could happen we will also need to check if it's a match

 4) Join and group by will also need a way to effectively search by only part of tuple, ie by specific fields 


 ok' this kinda makes sense, so now let's think again about storing indexes and count's
 
 
 Each Table will maintain it's own indexes, and they will only be used for garbage collection
 
##### This will work for Union, Except, Intersect
ok what if we stored: single <value,index> mapping to know what index to assign to given value, this also would be persistent and append only
<index, list<TupleLocation>> to store list of all tuples with given index in database- this will be sorted and never deleted
 
this way we could satisfy 1 by checking first mapping and then getting first matching from second mapping
satisfy 2 by first checking  first mapping than second mapping
be able to satisfy three by first checking first matching than second mapping 
 
##### For join and gruping functions we would need, so there would be single extra field to keep track of
<value, index> mapping to know what index to assign to given value
<index, list<TupleLocation>> to store list of all tuples with given index in database- this will be sorted indexes won't be deleted but lists will be trimmed
<specific fields, index> mapping to know which tuples match given constrain

#### and for cross join: we would only need
<value, index> mapping to know what index to assign to given value, rest we have to do iteratively
 

So what is api that we need to provide from Tables:

BatchInsert - ok

Iterate - returns all tuples one by one - ok

Get - this will return satisfying tuples in sorted order

HashSearch - it will search for tuples,
depending on node type we will use different map here either <specific fields, index>
or <value_index>


 										____________________________________
And finally our storage would keep  | index | timestamp | count | data | 
  								  	------------------------------------
where timestamp persist through computation, index is maintained by each tuple, and count is being updated continiously
 
Node can be of type:
 
###### Selection, which is sql substitute of
 
Select name, last_name, age from Osoby
Where age > 18 and age < 65
 
It's selection with WHERE
 
so we can see that our tuple for this node may need to store all of the fields
of ingres tuple, and it doesn't store any state(table)
 
doesn't care about state, doesn't care about count, doesn't care about index or timestamps, just compuuuutes on data field
 
###### Projection, which is sql substitute of
 
 Select name, last_name, age from Osoby
 
 It's selection but without WHERE
 
 so we can see thah our tuple for this node may need to store all of the fields
 of ingres tuple, and it does not store any state(table)
 
 doesn't care about state, doesn't care about count, doesn't care about index or timestamps, just compuuuutes on data field
 
###### Union, which is sql substitute of
 
 SELECT column1, column2, ...
   FROM table1
   UNION
   SELECT column1, column2, ...
   FROM table2;
 
 It combines tuples from two tables removing duplicate values
 
 select profesion from workers
 union
 select profession from profession
 
 and then we remove single worker with given proffesion from table1, 
 then union should probably only remove it if there are 0 left.
 
 So we should produce output of:  Timestamp | index| previous_count -1 | - this will be the correct state  
 or when we insert we should do the same but with	   previous_count + 1 instead 
 
 we will want to process both queuues at once, because we process graph in topological order, so both inputs are ready
 we can first merge queues to update corresponding count's and then do single insert to table and then write it to the output
  
 ok what if we would do this :
 tuples comes in either from right or left:
 	insert them all into table and put it into out_queue with updated count, and right timestamps
  
 
###### Intersect which is sql substitute of
 
 SELECT column1, column2, ...
  FROM table1
 INTERSECT
 SELECT column1, column2, ...
   FROM table2;
 
 It combines tuples from two tables, only passing those that appeard in both
 we first process first, inserted it into table and written it into out_queue,
 then processed second one, also write it into table and then update result in out_queue if needed
 
 keep two tables one for each source
 so first we insert into tables, than perform cross check on both right and left and put into out 
 
 count should be left_count right_count 
 
###### Difference
 
 SELECT column1, column2, ...
   FROM table1
   EXCEPT
   SELECT column1, column2, ...
   FROM table2;
 
 It passes tuple's that doesn't appear in second table
 
 it needs state, to know which tuples were present in table 2
 it doesn't need to know, fields
 it only needs to store single table
 
 and it will also need's to store table nr 1 to know what count to emit when we delete single value from table nr 1
 
 It combines tuples from two tables, only passing those that appeard in both
 we first process first, inserted it into table and written it into out_queue,
 then processed second one, also write it into table and then update result in out_queue if needed
 
 
 regarding count when second becomes negative new tuple will have - total_count of first,
 otherwie it will have + total_count 
 
###### Product
 
 // this is projection
 SELECT Employees.ID, Employees.Name, Employees.Department,
      Projects.Project_ID, Projects.Project_Name
   FROM Employees
 
   CROSS JOIN Projects
 	
	 // this is filter
   WHERE Employees.Department = 'IT' AND Projects.Project_Name = 'AI Development';
 
 It takes two tables as ingerses and produce X product
 it needs to store two Tables
 and it doesn't need to know their fields
 
 It combines tuples from two tables, only passing those that appeard in both
 we first process first, inserted it into table and written it into out_queue,
 then processed second one, also write it into table and then update result in out_queue if needed
 
 count will be multiplied of first and second
 
###### Join
 
 SELECT Employees.Name, Departments.Dept_Name
   FROM Employees
 JOIN Departments
 ON Employees.Department = Departments.Dept_ID;
 
 WE need to keep both inputs in tables
 
 It combines tuples from two tables, only passing those that appeard in both
 we first process first, inserted it into table and written it into out_queue,
 then processed second one, also write it into table and then update result in out_queue if needed
 
 
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
 
 ok so in general it's stateful single in queue single out_queue, that appiles function to given rows,
 
 it will create tuple with data | group_by field | rest of fields with appiled function
 
 
 but the catch is we will need to search etc. tuples only by aggregate fields
 
 
 How will we actually perform computation and maintaint consistency?
 
 How will we handle update delete insert?
 
 How will we garbage collect?
 
 How will we iterate our tables to get all matching inputs for say, joins
 
 we will store all nodes for all queries in single graph
 graph will we defined as set of connected nodes & N workers
 
 For each node we will let input accumulate in it's input queue,
 after some time, or amount of Tuple's in input queue we will start computation,
 in topological order.

##### Table

  On disk tuples will also hold InTableIndex of next(older version) of tuple


	// Tables are sorted by <timestamp|index>


	// now let's think how to efficiently implement our required operations:
	// tuple will be computed against all versions newer than itself or just newest one if it's the newest



	// update timestamp
	/*
		for given tuple if it's first and it's pointer is less than ts -> remove all older
		else travel to next version of this tuple and repeat
	*/

	// compaction
	/*
		let's assume they will only happen on shut down, could be also after X hours for some systems, but
		we won't implement that, keep only most recent version,
		travel pages by two at once compact them, and overlaping switch to next two pages

	
 
 
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
 
 
 ##### How we will use nodes in graph creation
 
  firstly we need global graph object, that worker's wiil process in topological order
 
  So creating all nodes we need to put all of them onto graph,
  therefore we won't create nodes directly, but instead we will do something like
  auto graph = Graph()
 
  .. or first create global graph object and pass it in constructor
 
 
 auto in_1 = graph.input_node(constructor args...)
 auto in_2 = graph.other_input(constructor args...)
 
 auto in_1 = input_node.filter(function).project(function)
 auto in_2 = input_node_2.filter(function).project(function)
 auto out = join(in_1, in_2)
 
 auto result =
  graph.join(function,
      project(function,
               filter(function, input_node)),
      project(function,
               filter(function, input_node_2)))
 
  Do we need types and dynamics' node creation?
 
  dynamic node creation would be cool, types not necessary needed
 
 
 let's say node also holds vector of enum fields, like:
 enum Field = {STRING, FLOAT, INT, BOOL},
 which will correspond to
 
 
##### Node's api:
 
  Compute: this will perform all computable action <for now> that is:
 
       Updating timestamp of internal tables possibly with garbage collection
       Computing out_tuples
 
   Output: return out_queue of given node: this will be used by graph layer for chaining nodes
 
 
   UpdateTimestamp: this will be called by output nodes it will update ts and propagate to input nodes
   updating table state is more costly and will be handled by worker threads
 
 
 
 ##### All automatically created functions will work on data part of tuples
 without timestamp indexes, and op metadata
 




#### Storage:

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




#### Sources

(cmu 15-721)[https://15721.courses.cs.cmu.edu/spring2024/slides/07-compilation.pdf]

(unixism)[https://unixism.net/loti/tutorial/fixed_buffers.html]

https://www.vldb.org/pvldb/vol7/p853-klonatos.pdf

dbsp

https://www.skyzh.dev/blog/2023-12-28-store-of-streaming-states/

[dida why] https://github.com/jamii/dida/blob/main/docs/why.md


## Develomnept Commands

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