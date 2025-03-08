#ifndef ALICEDBWORKERPOOL
#define ALICEDBWORKERPOOL

#include "Common.h"
#include "Graph.h"

#include <algorithm>
#include <atomic>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

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
 * maybe instead submit all tasks at once, when thread dependencies are processed, put future ready on future tasks
 * use 1 thread per 1 graph
 *
 *
 * use lock free queue to submit and pop requests
 */

/**
 * New design:
 * each node will return boolean after compute true - at least one tuple was inserted to outqueue, false otherwise
 *
 * and we will store state for each node of (bool, processed_state) and then we will check nodes that depends on it,
 * if such node has other dependency already fulfilled, we will add itself to available queue to be processed,
 * if other dependency is already fulfilled we will be grab it instantly
 * or set it to processed instantly if both are false
 *
 *
 * and now for sleeping / waiting
 * 	We can sleep some time on all threads if there is no work to do
 *
 *
 * and for optimal performance we want guarantee that there is no less graphs than threads
 */

struct GraphState {
	Graph *g_;
	std::shared_mutex shared_lock_;
};

class WorkerPool {
public:
	explicit WorkerPool(int workers_cnt = 1) : workers_cnt_ {workers_cnt}, stop_all_ {false} {
		for (int i = 0; i < this->workers_cnt_; i++) {
			this->workers_.emplace_back(&WorkerPool::WorkerThread, this, i);
		}
	}

	~WorkerPool() {
		this->StopAll();
		for (auto &worker : this->workers_) {
			if (worker.joinable()) {
				worker.join();
			};
		}
	}

	void StopAll() {
		this->stop_all_ = true;
	}

	// remove graph g from being processed by worker poll
	/** @todo if after stop there will be more workers than threads stop some worker */
	void Stop(Graph *g) {
		std::unique_lock lock(graphs_lock_);
		for (auto it = this->graphs_.begin(); it != this->graphs_.end(); it++) {
			if ((*it)->g_ == g) {
				(*it)->shared_lock_.lock();
				auto state = *it;
				this->graphs_.erase(it);
				state->shared_lock_.unlock();
				break;
			}
		}
	}

	void Start(Graph *g) {
		std::unique_lock lock(this->graphs_lock_);
		if (g) {
			g->Start();
		}
		// this->graphs_lock_.lock();
		for (const auto &state : graphs_) {
			if (state->g_ == g) {
				return;
			}
		}
		graphs_.emplace_back(std::make_shared<GraphState>(g));
	}

private:
	void WorkerThread(int index) {
		try {
			// process untill stop is called on this thread, or on all threads
			while (!this->stop_all_) {
				auto task = this->GetWork();
				if (!task) {
					std::this_thread::yield();
					//  no graphs, go to sleep
					// std::this_thread::sleep_for(std::chrono::seconds(1));
					continue;
				}
				Node *n;
				if (!task->g_->GetNext(&n)) {
					// no work to be done for this graph, continue
					task->shared_lock_.unlock_shared();
					// pick some better way
					std::this_thread::yield();
					// std::this_thread::sleep_for(std::chrono::seconds(1));

					continue;
				}
				// process this node
				bool produced = n->Compute();
				task->g_->Produced(n, produced);
				task->shared_lock_.unlock_shared();
				// if there is no work left to do find mechanism for waiting
			}
		} catch (const std::exception &e) {
			std::cerr << "[Error] Exception in worker thread: " << e.what() << std::endl;
			// handle exception
		}
	}

	/**
	 * @brief Iterates all graphs, and returns next graph with work to do
	 *  called by worker thread that is currently free, to get new work assigned
	 * @return function that needs to be performed
	 */
	std::shared_ptr<GraphState> GetWork() {
		// waits to graph locks, but it's held by thread that is waiting for current thread to end
		std::scoped_lock lock(graphs_lock_);
		size_t g_size = this->graphs_.size();

		if (g_size == 0) [[unlikely]] {
			return nullptr;
		}

		unsigned int idx = next_index_.fetch_add(1);
		idx = idx % g_size;
		// std::cout<<" idx : " << idx <<	 std::endl;
		this->graphs_[idx]->shared_lock_.lock_shared();
		return this->graphs_[idx];
	}

	// all graphs that we are processing
	std::vector<std::shared_ptr<GraphState>> graphs_;
	std::shared_mutex graphs_lock_;
	// we want to prevent situation where worker acquired graph node to be processed, and before
	// calling compute on it, graph get's removed
	// index next graph to be processed
	std::atomic<unsigned int> next_index_ {0};

	std::vector<std::thread> workers_;

	bool stop_all_;

	const int workers_cnt_;
};

} // namespace AliceDB
#endif