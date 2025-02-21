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
 * @brief initial simple worker pool implementation
 * we keep:
 *  at least 1 worker thread
 *  if there are G graphs there should be at most N <= G threads,
 *  thread i is responsible for processing G % N == i graphs,
 */

struct GraphState {
	Graph *g_;
	std::shared_mutex shared_lock_;
};

class WorkerPool {
public:
	explicit WorkerPool(int max_workers_cnt = 1)
	    : max_workers_cnt_ {max_workers_cnt > 0 ? max_workers_cnt : 1}, workers_cnt_ {0} {
		this->stop_worker_.resize(this->max_workers_cnt_);
		for (int i = 0; i < this->max_workers_cnt_; i++) {
			this->stop_worker_[i] = true;
		}
	}

	~WorkerPool() {
		this->StopAll();
		std::lock_guard<std::mutex> lock(threads_lock_);
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
		this->graphs_lock_.lock();
		// this->graphs_lock_.lock();
		for (auto it = this->graphs_.begin(); it != this->graphs_.end(); it++) {
			if ((*it)->g_ == g) {
				(*it)->shared_lock_.lock();
				auto state = *it;
				this->graphs_.erase(it);
				state->shared_lock_.unlock();
				/** @todo maybe not delete but just stop processing it and still make accesible? */
				break;
			}
		}
		this->graphs_lock_.unlock();
		/*	
		this->threads_lock_.lock();
		if (this->workers_cnt_ > this->graphs_.size()) {
			int i = this->workers_cnt_ - 1;
			if (this->workers_[i].joinable()) {
				this->workers_cnt_--;
				this->stop_worker_[i] = true;
				this->workers_[i].join();
			}
			this->workers_.pop_back();
		}
		this->threads_lock_.unlock();
		*/
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


		this->threads_lock_.lock();
		if (this->workers_cnt_ < this->graphs_.size() && this->workers_cnt_ < this->max_workers_cnt_) {
			this->stop_worker_[workers_cnt_] = false;
			this->workers_.emplace_back(&WorkerPool::WorkerThread, this, workers_cnt_);
			this->workers_cnt_++;
		}
		this->threads_lock_.unlock();
	}

	void WorkerThread(int index) {
		try {
			// process untill stop is called on this thread, or on all threads
			while (!this->stop_worker_[index] && !this->stop_all_) {
				auto task = this->GetWork();
				if (!task) {
					return;
				}
				Node *n;
				if (!task->g_->GetNext(&n)) {
					// no work to be done for this graph, continue
					task->shared_lock_.unlock_shared();
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
	std::shared_ptr<GraphState> GetWork() {
		// waits to graph locks, but it's held by thread that is waiting for current thread to end
		std::shared_lock lock(graphs_lock_);
		size_t g_size = this->graphs_.size();
		
		if (g_size == 0) [[unlikely]] {
			return nullptr;
		}
		
		int index = this->next_index_;
		next_index_ = (next_index_ + 1) % g_size;

		this->graphs_[index]->shared_lock_.lock_shared();
		return this->graphs_[index];
	}

	// all graphs that we are processing
	std::vector<std::shared_ptr<GraphState>> graphs_;
	std::shared_mutex graphs_lock_;
	// we want to prevent situation where worker acquired graph node to be processed, and before
	// calling compute on it, graph get's removed
	// index next graph to be processed
	int next_index_ = 0;

	std::vector<std::thread> workers_;
	std::vector<bool> stop_worker_;

	std::mutex threads_lock_;
	bool stop_all_ = false;

	int workers_cnt_;
	const int max_workers_cnt_;
};

} // namespace AliceDB
#endif