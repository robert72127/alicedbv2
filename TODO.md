## Nodes [v]:
    *   make sure algorithm is correct - fix joins [V]

## Cache [v]:
    * Think again whether it's implementation is good, maybe change it a bit, for example maybe keep track of size and resize it down [V]

## DataLoaders [v]:
    *   Implement dataloaders for other sources such as network [V]
    *   Make it part of graph source node for easier definitions, provide some unified interface of file or something [v]

## WorkerPool: [v]
    *   implement proper graph scheduling [v]
    *   maybe assign number to each node and number to each graph so that we will be able to process single graph by multiple workers [v]
    *   add mechanism to dynamically stop and start new threads [v]


## State Persistence:
    * Implement B+Tree, and Storage class []
        * write code working without b+tree, heap based, test whole system [v]
        * add b-tree indexes []
            * for now use recomputed in memory structure
        * add garbage collection [v] 
    * Add metadata loading/storing [v]

    * Add correct destructors [V]:
        * first we call destructor on worker pool,
        this stops graphs,
        * then we call destructor on graph,
        this will save metadata state
        * lastly we call destructor on bufferpool

    * Correctly identify structure of files that needs to be stored [V]
    * Wrap everything yet another time [V]


    
## Tests []:
    * write tests for each node [V]
    * write tests for the whole system, that performs both inserts and deletes of tuples []

## Fix:
    WorkerPoolTest []

## Add:
    Exporting data []
----------------------------------------------------------------------------------------------

## Write PDF with description