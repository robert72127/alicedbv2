Storage:


## WorkerPool:
    *   implement proper graph scheduling
    *   maybe assign number to each node and number to each graph so that we will be able to process single graph by multiple workers
    *   add mechanism to dynamically stop and start new tasks

## Storage:
    *   Either find good ready to use or implement persistent storage primitive

## DataLoaders:
    *   Implement dataloaders for other sources such as kafka and network

## Nodes:
    *   make sure algorithm is correct
    *   maybe add fixed point iteration but then timestamps would need to be vector and that would mean storage would be harded to implement, thus it's not perfect idea

## Write PDF with description