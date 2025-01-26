## Nodes:
    *   make sure algorithm is correct - fix joins [V]

## Cache:
    * Think again whether it's implementation is good, maybe change it a bit, for example maybe keep track of size and resize it down [V]

## DataLoaders:
    *   Implement dataloaders for other sources such as network [V]

## WorkerPool: [v]
    *   implement proper graph scheduling [v]
    *   maybe assign number to each node and number to each graph so that we will be able to process single graph by multiple workers [v]
    *   add mechanism to dynamically stop and start new threads [v]



------------------------------------------- Zrobić ten etap do końca tygodnia 

## Storage:
    *   Either find good ready to use or implement persistent storage primitive
    *   Use alicedbV1 with added B+Tree for key|value mapping
    *   Use rocksdb for delta storage for also standard storage with b+ tree since it also grants prefix search for free
    *   allow for exporting view to some format & dropping database graph


## Use some proper metafile to store metadata for all the classes

## Write PDF with description