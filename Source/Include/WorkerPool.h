#ifndef ALICEDBWORKERPOOL
#define ALICEDBWORKERPOOL

#include <vector>
#include <iostream>
#include <thread>
#include <algorithm>
#include <mutex>

#include "Graph.h"

namespace AliceDB{


/**
 * @brief initial simple worker pool implementation
 * we keep:
 *  at least 1 worker thread
 *  if there are G graphs there are at most N < G threads,
 *  thread i is responsible for processing G % N == i graphs,
 */
class WorkerPool{
    public:

    WorkerPool(int max_thread_cnt=1): max_thread_cnt_{max_thread_cnt}, thread_cnt_{0} {}

    ~WorkerPool(){
        this->stop_ = true;
        for(int i = 0 ; i < this->thread_cnt_; i++){
            this->threads_[i].join();
        }
    }

    void StopAll(){ this->stop_ = true;}

    // remove graph g from being processed by worker poll
    void Stop(Graph *g){
        for(auto it = this->graphs_.begin(); it != this->graphs_.end(); it++){
            if (*it == g){
                this->graphs_.erase(it);
            }
        }
    }

    void Start(Graph *g){
        this->latch_.lock();
        graphs_.push_back(g);

        // optionally create new thread
        if(this->thread_cnt_ == 0 || ( this->graphs_.size() > this->thread_cnt_ * 2 && this->thread_cnt_ < this->max_thread_cnt_ )){
            this->threads_.emplace_back(&WorkerPool::WorkerThread, this, thread_cnt_);
            this->thread_cnt_++;
        }

        this->latch_.unlock();
    }



    void WorkerThread(int index){
        try{
            // process untill stop is called
            while(!this->stop_){
                // process all graphs assigned to this thread
                for(int i = index; i < graphs_.size(); i+= index){
                    graphs_[i]->Process(1);
                }
                // if there is no work left to do find mechanism for waiting
            }
        } catch (const std::exception &e) {
		    std::cerr << "[Error] Exception in worker thread: " << e.what() << std::endl;
            // handle exception
        }
    }


private:

    // all graphs that we are processing
    std::vector<Graph*> graphs_;
    std::vector<std::thread> threads_;
    bool stop_ = false;

    int thread_cnt_;
    const int max_thread_cnt_;

    std::mutex latch_;
};

}
#endif