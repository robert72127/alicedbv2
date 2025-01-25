#ifndef ALICEDBWORKERPOOL
#define ALICEDBWORKERPOOL

#include <algorithm>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <atomic>

#include "Graph.h"
#include "Common.h"

namespace AliceDB {
/*
  We can process many graphs in parallel that's first point
  Second point is that we can actually process any level K in parallel for given graph,
  since we are sure those nodes are independent.
  And finally when we still process level K we can process level K+1 node if we know all it's
  dependencies from level < K+1 are already processed, so now for new algorithm:


  for each node in graph keep binary field processed?,

  assign nodes at level 0 to worker pool,

  if all processed at level K append level k+1,

  after worker ends processing node check if it's out nodes can be processed

  if all nodes in graph are visited processed, set processed to true?

*/

/**
 * @brief initial simple worker pool implementation
 * we keep:
 *  at least 1 worker thread
 *  if there are G graphs there are at most N < G threads,
 *  thread i is responsible for processing G % N == i graphs,
 */
class WorkerPool {
 public:
  WorkerPool(int thread_cnt = 1) :  thread_cnt_{thread_cnt} 
  {

    for(int i =0 ; i < this->thread_cnt_; i++){
        this->threads_.emplace_back(&WorkerPool::WorkerThread, this, thread_cnt_);
    }
  }

  ~WorkerPool() {
    this->stop_ = true;
    for (int i = 0; i < this->thread_cnt_; i++) {
      this->threads_[i].join();
    }
  }

  void StopAll() { 
    this->stop_ = true; 
  }

  // remove graph g from being processed by worker poll
  void Stop(Graph *g) {
    this->spin_lock_.lock();
    for (auto it = this->graphs_.begin(); it != this->graphs_.end(); it++) {
      if (*it == g) {
        this->graphs_.erase(it);
      }
    }
    this->spin_lock_.unlock();
  
  }

  void Start(Graph *g) {
    g->Start();
    this->spin_lock_.lock();
    graphs_.push_back(g);
    this->spin_lock_.unlock();
  }

  void WorkerThread(int index) {
    try {
      // process untill stop is called
      while (!this->stop_) {
        Graph *g =  this->GetWork();
        Node *n;
        if(! g->GetNext(&n)){
          // no work to be done for this graph, conitnue
          continue;
        }
        // process this node
        n->Compute();
        g->SetState(n, NodeState::PROCESSED);
        // if there is no work left to do find mechanism for waiting
      }
    } catch (const std::exception &e) {
      std::cerr << "[Error] Exception in worker thread: " << e.what() << std::endl;
      // handle exception
    }
  }

 private:
  /**
   * @brief Iterates all graphs, and returns next graph with work to do
   *  called by worker thread that is currently free, to get new work assigned
   * @return function that needs to be performed
   */
  Graph *GetWork(){
    this->spin_lock_.lock();
    int index = this->next_index_;
    next_index_ = next_index_ < graphs_.size() ? next_index_ + 1 : 0; 
    this->spin_lock_.unlock();

    return this->graphs_[index];
  }

  // all graphs that we are processing
  std::vector<Graph *> graphs_;
  //index next graph to be processed
  int next_index_;
  SpinLock spin_lock_;

  std::vector<std::thread> threads_;
  bool stop_ = false;

  int thread_cnt_;
};

}  // namespace AliceDB
#endif