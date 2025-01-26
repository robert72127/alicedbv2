#ifndef ALICEDBWORKERPOOL
#define ALICEDBWORKERPOOL

#include <algorithm>
#include <iostream>
#include <mutex>
#include <shared_mutex>
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
 *  if there are G graphs there should be at most N <= G threads,
 *  thread i is responsible for processing G % N == i graphs,
 */

struct GraphState{
  Graph * g_;
  std::shared_mutex shared_lock_;
};

class WorkerPool {
 public:
  explicit WorkerPool(int max_thread_cnt = 1) :  max_thread_cnt_{max_thread_cnt > 0? max_thread_cnt : 1}, threads_cnt_{0} 
  {
    this->next_index_ = 0;
    for(int i =0 ; i < this->threads_cnt_; i++){
    }
  }

  ~WorkerPool() {
    this->StopAll();
    for(auto &t: this->threads_){
      if (t.joinable()) {t.join(); };
    }
  }

  void StopAll() { 
    this->stop_all_ = true; 
  }

  // remove graph g from being processed by worker poll
  /** @todo if after stop there will be more workers than threads stop some worker */
  void Stop(Graph *g) {
    this->lock_.lock();
    for (auto it = this->graphs_.begin(); it != this->graphs_.end(); it++) {
      if ((*it)->g_ == g) {
        (*it)->shared_lock_.lock(); 
        std::shared_ptr<GraphState> state = *it;
        this->graphs_.erase(it);
        state->shared_lock_.unlock();
        break;
      }
    }

    if(this->threads_cnt_ > this->graphs_.size()){
      int i = this->threads_cnt_-1;
      this->threads_cnt_--;
      this->stop_[i] = true;
      if(this->threads_[i].joinable()){
        this->threads_[i].join();
      }
      this->threads_.pop_back();
      this->stop_.pop_back();
    }
    this->lock_.unlock();
  
  }

  void Start(Graph *g) {
    g->Start();
    this->lock_.lock();
    graphs_.push_back( std::make_shared<GraphState>(g));
    if(this->threads_cnt_ < this->graphs_.size() && this->threads_cnt_ < this->max_thread_cnt_){
        this->threads_.emplace_back(&WorkerPool::WorkerThread, this, threads_cnt_);
        this->stop_.emplace_back(false);
        this->threads_cnt_++;
    }
    this->lock_.unlock();
  }

  void WorkerThread(int index) {
    try {
      // process untill stop is called on this thread, or on all threads
      while (!this->stop_[index] && !this->stop_all_) {
        auto task =  this->GetWork();
        if (!task){
          return;
        }
        Node *n;
        if(! task->g_->GetNext(&n)){
          // no work to be done for this graph, continue
          continue;
        }
        // process this node
        n->Compute();
        task->g_->SetState(n, NodeState::PROCESSED);
        task->shared_lock_.unlock_shared();
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
  std::shared_ptr<GraphState> GetWork(){
    this->lock_.lock();
    // should neven happen
    if (this->graphs_.size() == 0)  [[unlikely]]{
      this->lock_.unlock();
      return nullptr;
    }
    int index = this->next_index_;
    next_index_ = next_index_ + 1 < this->graphs_.size() ? next_index_ + 1 : 0; 
    this->graphs_[index]->shared_lock_.lock_shared();
    this->lock_.unlock();
    return this->graphs_[index];
  }

  // all graphs that we are processing
  std::vector<std::shared_ptr<GraphState>> graphs_;
  // we want to prevent situation where worker acquired graph node to be processed, and before calling compute on it, graph get's removed
  //index next graph to be processed
  int next_index_;
  std::mutex lock_;

  std::vector<std::thread> threads_;
  std::vector<bool> stop_;
  bool stop_all_ = false;

  int threads_cnt_;
  const int max_thread_cnt_;
};

}  // namespace AliceDB
#endif