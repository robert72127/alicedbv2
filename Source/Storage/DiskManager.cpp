#include "DiskManager.h"

#include "BufferPool.h"
#include "Common.h"

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <liburing.h>
#include <mutex>
#include <thread>
#include <unistd.h>

namespace AliceDB {

DiskManager::DiskManager(BufferPool *bp, const std::filesystem::path &file_path)
    : bp_ {bp}, file_path_ {file_path}, set_promise_ {true} {
	std::unique_lock<std::mutex> work_lock(this->work_mutex_);

	if (!bp_) {
		throw std::runtime_error("[Error] BufferPool pointer is null.");
	}

	if (std::filesystem::exists(this->file_path_)) [[likely]] {
		this->page_count_ = std::filesystem::file_size(this->file_path_) / PageSize;
	} else {
		this->page_count_ = 0;
	}
	this->fd_ = open(this->file_path_.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0660);
	if (this->fd_ == -1) [[unlikely]] {
		throw std::runtime_error("[Error] Unable to open database file: " + file_path.string() + "\n");
	}

	// initialize iouring
	int ret;
	ret = io_uring_queue_init(this->bp_->GetPageCount(), &(this->ring_), 0);
	if (ret) [[unlikely]] {
		throw std::runtime_error("[Error] Unable to setup io_uring: " + std::string(std::strerror(-ret)) + "\n");
	}

	// regiter file descriptor with io_uring,
	// eliminate requirement for lookup
	ret = io_uring_register_files(&(this->ring_), &this->fd_, 1);
	if (ret) [[unlikely]] {
		throw std::runtime_error(
		    "[Error] Can't register file descriptor with io_uring: " + std::string(std::strerror(-ret)) + "\n");
	}

	for (int i = 0; i < this->bp_->GetPageCount(); i++) {
		this->iov[i].iov_base = this->bp_->pages_metadata_[i]->mempage->GetData();
		this->iov[i].iov_len = PageSize;
	}

	// regiter buffers this causes kernel to map these buffers in
	ret = io_uring_register_buffers(&(this->ring_), this->iov, this->bp_->GetPageCount());
	if (ret != 0) [[unlikely]] {
		throw std::runtime_error("[Error] Can't register io_uring Buffer pool aborting.");
	}

	// create worker thread
	this->worker_ = std::thread(&DiskManager::WorkerThread, this);
}

DiskManager::~DiskManager() {
	std::unique_lock<std::mutex> buffer_lock(this->bp_->buffer_mutex_);

	// notify worker it's time to stop
	this->stop_ = true;
	this->condition_var_.notify_one();

	// stop worker thread
	if (this->worker_.joinable()) {
		this->worker_.join();
	}

	// write all dirty pages from buffer pool
	// more efective than calling write one by one

	std::unique_lock<std::mutex> lock(this->work_mutex_);

	this->set_promise_ = false;

	std::vector<index> requested_indexes;
	for (auto &pm : this->bp_->pages_metadata_) {
		if (pm->is_dirty) {

			requested_indexes.push_back(pm->in_memory_index);

			// create request
			auto req_ = std::make_unique<Request>();
			req_->page_metadata_ = pm;
			req_->op_ = DiskOp::Write;

			this->submit_work_requests_.emplace_back(std::move(req_));
			this->request_count_++;
		}
	}

	// flush all pages to disk
	this->perform_disk_op_ = true;
	this->condition_var_.notify_one();

	this->SubmitDiskRequest();

	work_mutex_.unlock();
	// Unregister registered files
	io_uring_unregister_files(&(this->ring_));

	// Unregister buffers
	io_uring_unregister_buffers(&(this->ring_));

	// exit io_uring_queue
	io_uring_queue_exit(&(this->ring_));

	// Close the file descriptor
	if (this->fd_ >= 0) {
		close(this->fd_);
	}

	/** @todo write holes numbers into some metadata file,
	 * later on when we will call DiskManager() again load hole numbers from it
	 *
	 */
}

std::future<bool> DiskManager::WritePage(const index &in_memory_pid) {
	if (in_memory_pid >= this->bp_->GetPageCount()) {
		std::promise<bool> promise;
		promise.set_value(false);

		return promise.get_future();
	}

	// create request
	auto req_ = std::make_unique<Request>();
	req_->page_metadata_ = this->bp_->pages_metadata_[in_memory_pid];
	req_->page_metadata_->promise_ = std::promise<bool>();
	req_->op_ = DiskOp::Write;

	if (req_->page_metadata_->disk_index >= this->page_count_) {
		req_->page_metadata_->promise_.set_value(false);
		return req_->page_metadata_->promise_.get_future();
	}

	// add request to request list
	std::future<bool> future = req_->page_metadata_->promise_.get_future();
	{
		std::unique_lock<std::mutex> lock(this->work_mutex_);

		this->submit_work_requests_.emplace_back(std::move(req_));
		this->request_count_++;

		// if threshold reached notify worker which will perform job

		if (this->request_count_ >= this->disk_op_threshold_) {
			this->perform_disk_op_ = true;
			// notify worker
			this->condition_var_.notify_one();
		}
	}

	// return promise
	return future;
}

std::future<bool> DiskManager::ReadPage(const index &in_memory_pid) {
	if (in_memory_pid >= this->bp_->GetPageCount()) {
		std::promise<bool> promise;
		promise.set_value(false);
		return promise.get_future();
	}

	// create request
	auto req_ = std::make_unique<Request>();
	req_->page_metadata_ = this->bp_->pages_metadata_[in_memory_pid];
	req_->page_metadata_->promise_ = std::promise<bool>();
	req_->op_ = DiskOp::Read;

	if (req_->page_metadata_->disk_index >= this->page_count_) {
		req_->page_metadata_->promise_.set_value(false);
		return req_->page_metadata_->promise_.get_future();
	}

	std::future<bool> future = req_->page_metadata_->promise_.get_future();
	// add request to request list
	{
		std::unique_lock<std::mutex> lock(this->work_mutex_);

		this->request_count_++;
		this->submit_work_requests_.emplace_back(std::move(req_));

		// if threshold reached notify worker which will perform job

		if (this->request_count_ >= this->disk_op_threshold_) {
			this->perform_disk_op_ = true;
			// notify worker
			this->condition_var_.notify_one();
		}
	}

	// return promise
	return future;
}

bool DiskManager::DeletePage(const index &on_disk_page_id) {
	std::unique_lock<std::mutex> lock(this->holes_mutex_);

	if (on_disk_page_id >= this->page_count_) {
		return false;
	}

	this->holes_.emplace_back(on_disk_page_id);
	this->holes_count_++;

	// check if we should notify worker
	if (this->holes_count_ / this->page_count_ > holes_threshold_) {
		this->compact_ = true;
		// notify worker
		this->condition_var_.notify_one();
	}

	return true;
}

bool DiskManager::Resize(size_t new_size) {

	if (new_size <= this->page_count_) {
		return false;
	}

	if (ftruncate(this->fd_, new_size * PageSize) != 0) {
		std::cerr << "[Error] resising file " << strerror(errno) << std::endl;
		return false;
	}

	this->page_count_ = new_size;
	return true;
}

index DiskManager::AllocatePage() {

	std::unique_lock<std::mutex> holes_lock(this->holes_mutex_);
	if (this->holes_count_ > 0) {
		index pindex = this->holes_.front();
		this->holes_.pop_front();
		this->holes_count_--;
		return pindex;
	}
	holes_lock.unlock();

	std::unique_lock<std::mutex> page_lock(this->page_mutex_);
	if (this->max_used_index_ < page_count_) {
		return (this->max_used_index_++);
	}

	// increase size twice, vector way
	this->Resize((this->page_count_ == 0) ? 1 : this->page_count_ * 2);
	return (this->max_used_index_++);
}

int DiskManager::SubmitDiskRequest() {

	unsigned head;
	unsigned i = 0;

	// prepare each request
	for (auto &req : perform_work_requests_) {

		// get SQE from io_uring
		struct io_uring_sqe *sqe = io_uring_get_sqe(&(this->ring_));
		if (!sqe) {
			req->page_metadata_->promise_.set_exception(
			    std::make_exception_ptr(std::runtime_error("Couldn't get io_uring SQE")));
			continue;
		}

		// Set the IOSQE_FIXED_FILE flag since we use preregister file descriptor
		sqe->flags |= IOSQE_FIXED_FILE;

		// Prepare the I/O operation based on req_->op_
		if (req->op_ == DiskOp::Read) {
			io_uring_prep_read_fixed(sqe, this->fd_, this->iov[req->page_metadata_->in_memory_index].iov_base, PageSize,
			                         req->page_metadata_->disk_index * PageSize, req->page_metadata_->in_memory_index);
		} else {
			io_uring_prep_write_fixed(sqe, this->fd_, this->iov[req->page_metadata_->in_memory_index].iov_base,
			                          PageSize, req->page_metadata_->disk_index * PageSize,
			                          req->page_metadata_->in_memory_index);
		}
		io_uring_sqe_set_data(sqe, req.get());
	}

	// submit all requests
	int ret = io_uring_submit(&(this->ring_));
	if (ret < 0) {
		// Handle submission error
		for (auto &req : this->perform_work_requests_) {
			req->page_metadata_->promise_.set_exception(
			    std::make_exception_ptr(std::runtime_error("Failed to submit io_uring requests")));
		}
		return ret;
	}

	// Wait for completions
	struct io_uring_cqe *cqe;
	unsigned int completed = 0;
	while (completed < this->perform_work_requests_.size()) {
		//	std::cout<<"Submited\n";
		ret = io_uring_wait_cqe(&(this->ring_), &cqe);
		if (ret < 0) {
			// Handle error: Set exceptions on remaining promises
			for (auto &remaining_req : this->perform_work_requests_) {
				if (remaining_req->page_metadata_->promise_.get_future().valid()) {
					remaining_req->page_metadata_->promise_.set_exception(
					    std::make_exception_ptr(std::runtime_error("Failed to wait for io_uring CQE")));
				}
			}
			return ret;
		}

		// Retrieve the associated request using user data
		Request *req = static_cast<Request *>(io_uring_cqe_get_data(cqe));

		if (cqe->res < 0) {
			// I/O operation failed
			req->page_metadata_->promise_.set_exception(std::make_exception_ptr(
			    std::runtime_error(std::string("I/O operation failed: ") + strerror(-cqe->res))));
		}
		// I/O operation succeeded
		else if (this->set_promise_) {
			req->page_metadata_->promise_.set_value(true);
		}

		io_uring_cqe_seen(&(this->ring_), cqe);
		completed++;
	}

	this->perform_work_requests_.clear();
	return 0;
}

/** @todo implement later when we will have tables and buffer pool*/
int DiskManager::Compact() {
	return true;
}

void DiskManager::WorkerThread() {
	try {
		while (true) {
			// sleep untill

			std::unique_lock<std::mutex> lock(this->work_mutex_);
			this->condition_var_.wait_for(lock, std::chrono::microseconds(this->sleep_duration_ms_),
			                              [this] { return this->stop_ || this->perform_disk_op_ || this->compact_; });

			if (this->stop_) {
				lock.unlock();
				break; // Exit the loop
			}
			/** @todo implement compaction  */
			/*
			if (this->compact_) {
			    lock.unlock();
			    if (Compact()) {
			        // @todo  maybe instead copy this file first and restore on fail, or
			        // assume it always work's
			        throw std::runtime_error("Compaction failed");
			    }
			}
			*/

			// either timeout or perform_disk_op is set to true
			else if (this->submit_work_requests_.size() > 0) {
				// while worker thread perform operation, requests can still be submitted
				std::swap(submit_work_requests_, perform_work_requests_);
				this->request_count_ = 0;
				this->perform_disk_op_ = false;
				lock.unlock();

				if (SubmitDiskRequest()) {
					std::runtime_error("Submitting disk operation failed\n");
				}
			}
		}
	} catch (const std::exception &e) {
		std::cerr << "[Error] Exception in worker thread: " << e.what() << std::endl;
		// Handle exception, possibly set a flag to indicate failure
	}
}

} // namespace AliceDB