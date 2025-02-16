#ifndef ALICEDBALICEDB
#define ALICEDBALICEDB

#include "BufferPool.h"
#include "DiskManager.h"
#include "Graph.h"
#include "WorkerPool.h"

#include <atomic>
#include <condition_variable>
#include <csignal>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdexcept>

namespace AliceDB {

class DataBase {
public:
	/** @brief Initialize shared graphs state, such as common directory, worker pool , buffer pool, disk manager */
	DataBase(std::filesystem::path database_directory, unsigned int worker_threads_count = 2,
	         GarbageCollectSettings *gb_settings = nullptr)
	    : database_directory_(database_directory), graph_count_ {0} {

		// check if directory exists
		//  if not create it,
		if (!std::filesystem::exists(database_directory)) {
			if (!std::filesystem::create_directories(database_directory)) {
				throw std::runtime_error("Failed to create directory: " + database_directory.string());
			}
		}

		// Create BufferPool
		this->bp_ = new BufferPool();

		// Build the full path for the database file.
		std::filesystem::path db_path = database_directory / "database.db";

		// Initialize disk manager and WorkerPool
		this->dm_ = new DiskManager(bp_, db_path.string());
		// set disk manager for buffer pool
		if (!this->bp_->SetDisk(dm_)) {
			throw std::runtime_error("Error while setting disk manager for buffer pool\n");
		}

		this->pool_ = new WorkerPool(worker_threads_count);

		/** @todo decide on default garbage collector policy */
		if (!gb_settings) {
			gb_settings = new GarbageCollectSettings {
			    .clean_freq_ = 5000, .use_garbage_collector = false, .remove_zeros_only = true};
		}
		this->gb_settings_ = gb_settings;
	}

	void Shutdown() {
		if (!shutdown_) {
			// 1 stop workerpool
			if (pool_) {
				this->pool_->StopAll();
				delete this->pool_;
				this->pool_ = nullptr;
			}

			// 2 call destructors on the graph this will save metadata state
			for (auto g : this->graphs_) {
				delete g;
			}
			this->graphs_.clear();

			// 3 call destructor on database file and buffer pool
			delete this->dm_;
			this->dm_ = nullptr;
			delete this->bp_;
			this->bp_ = nullptr;
			delete gb_settings_;
			this->gb_settings_ = nullptr;
			shutdown_ = true;
		}
	}

	~DataBase() {
		if (!this->shutdown_) {
			this->Shutdown();
		}
	}

	/** @brief, creates new graph instance */
	Graph *CreateGraph() {
		std::filesystem::path graph_directory = this->database_directory_ / ("graph_" + std::to_string(graph_count_));

		// create graph directory if it doesn't exists
		if (!std::filesystem::exists(graph_directory)) {
			try {
				std::filesystem::create_directory(graph_directory);
			} catch (const std::filesystem::filesystem_error &e) {
				std::cerr << "Error creating directory: " << e.what() << '\n';
			}
		}

		// create new graph and return it
		Graph *g = new Graph(graph_directory.string(), bp_, *this->gb_settings_);

		this->graphs_.emplace_back(g);
		graph_count_++;

		return g;
	}

	void StartGraph(Graph *g) {
		if (pool_) {
			pool_->Start(g);
		}
	}

	void StopGraph(Graph *g) {
		if (pool_) {
			pool_->Stop(g);
		}
	}

private:
	std::filesystem::path database_directory_;
	// Shared resources:
	BufferPool *bp_;
	DiskManager *dm_;
	WorkerPool *pool_;
	GarbageCollectSettings *gb_settings_;

	std::vector<Graph *> graphs_;
	unsigned int graph_count_;

	bool shutdown_ = false;
};
} // namespace AliceDB

#endif