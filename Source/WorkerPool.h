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
	explicit WorkerPool(int workers_cnt = 1) : workers_cnt_ {workers_cnt}, stop_all_{0} {
		for (int i = 0; i < this->workers_cnt_; i++) {
			this->workers_.emplace_back(&WorkerPool::WorkerThread, this, i);
		}
	}

	~WorkerPool() {
		if(this->stop_all_ == 0){
		}
		this->StopAll();
}

	void StopAll() {
		{
			std::scoped_lock lock(this->control_lock_);
			auto stop_all = [this](){
				this->stop_all_ = true;
			};
			this->control_tasks_.push_back(stop_all);
		}

		// wait for controll function to be submitted then join workers
		for (auto &worker : this->workers_) {
			if (worker.joinable()) {
				worker.join();
			};
		}

	}

	// remove graph g from being processed by worker poll
	void Stop(Graph *g) {

		std::scoped_lock lock(this->control_lock_);

		auto stop_g = [this,g](){
			if(!g){
				return;
			}
			for (auto it = this->graphs_.begin(); it != this->graphs_.end(); it++) {
				if ((*it) == g) {
					this->graphs_.erase(it);
					break;
				}
			}
		};
		this->control_tasks_.push_back(stop_g);
	}

	/** @brief  add g to workerpool work sources*/
	void Start(Graph *g) {
		std::scoped_lock lock(this->control_lock_);

		auto start_g = [this, g](){
			if(!g){
				return;
			}
			g->Start();
			
			// this->graphs_lock_.lock();
			for (const auto &present : graphs_) {
				if (present == g) {
					return;
				}
			}
			this->graphs_.emplace_back(g);
		};

		this->control_tasks_.push_back(start_g);
	}

private:
	/**
	 * @brief worker thread loop, acquires works, and then updates processed node state
	 */
	void WorkerThread(int index) {
		try {
			// process untill stop is called on this thread, or on all threads
			while (!this->stop_all_) {
				// get work

				auto [node, graph] = this->GetWork();
				if (node == nullptr) {
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
		while (this->work_queue_.empty()) {
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
			this->perform_control_task();
			
			// time to stop we need to actually return now
			if(this->stop_all_ == 1){
				return {nullptr, nullptr};
			}

			if (!any_produced) {
				// todo sleep for some time on this thread and for all others wait, maybe on cond var? or will current design force them to actually sleep on scoped_lock?
				std::this_thread::sleep_for(std::chrono::milliseconds(200));
			}
		}

		this->perform_control_task();
		auto ret = this->work_queue_.back();
		work_queue_.pop_back();
		return ret;
	}

	void perform_control_task(){
		std::scoped_lock lock(this->control_lock_);
		while(!this->control_tasks_.empty()){
				auto &task = this->control_tasks_.back();
				this->control_tasks_.pop_back();	
				task();
		}
	}

	// all graphs that we are processing
	std::vector<Graph *> graphs_;
	std::mutex graphs_lock_;
	// we want to prevent situation where worker acquired graph node to be processed, and before
	// calling compute on it, graph get's removed
	// index next graph to be processed
	std::atomic<unsigned int> next_index_ {0};

	std::vector<std::thread> workers_;

	bool stop_all_;

	const int workers_cnt_;

	std::deque<std::pair<Node *, Graph *>> work_queue_;

	std::deque<std::function<void()>> control_tasks_ = {};
	std::mutex control_lock_;

};

} // namespace AliceDB
#endif