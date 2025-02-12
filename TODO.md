## Nodes [v]:
    *   make sure algorithm is correct - fix joins [V]

## Cache [v]:
    * Think again whether it's implementation is good, maybe change it a bit, for example maybe keep track of size and resize it down [V]

## DataLoaders [v]:
    *   Implement dataloaders for other sources such as network [V]

## WorkerPool: [v]
    *   implement proper graph scheduling [v]
    *   maybe assign number to each node and number to each graph so that we will be able to process single graph by multiple workers [v]
    *   add mechanism to dynamically stop and start new threads [v]


## Storage:
    * Implement B+Tree, and Storage class []
    * Add metadata loading/storing [v]
    * Add correct destructors [V]:
        ok on sigkill etc 
        * first we call destructor on worker pool,
        this stops graphs,
        * then we call destructor on graph,
        this will save metadata state
        * lastly we call destructor on bufferpool
    * Add Signal Handler for gracefull shutdown []
----------------------------------------------------------------------------------------------


## Write PDF with description