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

class WorkerPool {
public:
	explicit WorkerPool(int workers_cnt = 1) : workers_cnt_ {workers_cnt} {
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
		this->stop_all.store(true);
	}

	// remove graph g from being processed by worker poll
	void Stop(Graph *g) {
		std::unique_lock lock(graphs_lock_);
		for (auto it = this->graphs_.begin(); it != this->graphs_.end(); it++) {
			if ((*it) == g) {
				this->graphs_.erase(it);
				break;
			}
		}
	}

	/** @brief  add g to workerpool work sources*/
	void Start(Graph *g) {
		std::unique_lock lock(this->graphs_lock_);
		if (g) {
			g->Start();
		}
		// this->graphs_lock_.lock();
		for (const auto &present : graphs_) {
			if (present == g) {
				return;
			}
		}
		graphs_.emplace_back(g);
	}

private:
	/**
	 * @brief worker thread loop, acquires works, and then updates processed node state
	 */
	void WorkerThread(int index) {
		try {
			// process untill stop is called on this thread, or on all threads
			while (!this->stop_all.load()) {
				// get work

				auto [node, graph] = this->GetWork();
				if (node == nullptr) {
					// std::this_thread::yield();
					std::this_thread::sleep_for(std::chrono::milliseconds(200));
					continue;
				}

				node->Compute();
				// we somehow must know g, maybe we should store pairs of node and graph pointers
				graph->Produced(node);
			}
		} catch (const std::exception &e) {
			std::cerr << "[Error] Exception in worker thread: " << e.what() << std::endl;
			// handle exception
		}
	}

	/**
	 * @brief This method is repsonsible for getting nodes from graphs and scheduling them to be processed
	 * @return works to be done by calling worker thread
	 */
	std::pair<Node *, Graph *> GetWork() {
		// waits to graph locks, but it's held by thread that is waiting for current thread to end
		std::scoped_lock lock(graphs_lock_);

		// populate work_queue
		if (this->work_queue_.empty()) {
			bool any_produced = false;
			for (const auto g : this->graphs_) {
				std::vector<Node *> nodes = g->GetNext();
				if (!nodes.empty()) {
					any_produced = true;
				}
				for (auto node : nodes) {
					this->work_queue_.push_back({node, g});
				}
			}

			if (!any_produced) {
				return {nullptr, nullptr};
			}
		}

		auto ret = this->work_queue_.back();
		work_queue_.pop_back();
		return ret;
	}

	// all graphs that we are processing
	std::vector<Graph *> graphs_;
	std::shared_mutex graphs_lock_;
	// we want to prevent situation where worker acquired graph node to be processed, and before
	// calling compute on it, graph get's removed
	// index next graph to be processed
	std::atomic<unsigned int> next_index_ {0};

	std::vector<std::thread> workers_;

	std::atomic<bool> stop_all {0};

	const int workers_cnt_;

	std::deque<std::pair<Node *, Graph *>> work_queue_;
};

} // namespace AliceDB
#endif