## Nodes:
    *   make sure algorithm is correct - it seems to be, maybe tweak time per node in source node, but besides that it's correct

## DataLoaders:
    *   Implement dataloaders for other sources such as kafka and network, if api to complicated use simpler one

## WorkerPool:
    *   implement proper graph scheduling
    *   maybe assign number to each node and number to each graph so that we will be able to process single graph by multiple workers
    *   add mechanism to dynamically stop and start new tasks

## Queue:
    * Think again whether it's implementation is good, maybe change it a bit

------------------------------------------- Zrobić ten etap do końca tygodnia ( wziąć wolny czwartek i piątek )

## Storage:
    *   Either find good ready to use or implement persistent storage primitive
    *   Use alicedbV1 with added B+Tree for key|value mapping
    *   Use rocksdb for delta storage
    *   allow for exporting view to some format & dropping database graph


## Write PDF with description