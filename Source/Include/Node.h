#ifndef ALICEDBNODE
#define ALICEDBNODE

#include "Common.h"
#include "Queue.h"

#include <map>
#include <list>
#include <functional>
#include <mutex>
#include <cstring>

#define DEFAULT_QUEUE_SIZE (200)

namespace AliceDB{


class Node {

public:
	virtual ~Node() {};

	virtual void Compute() = 0;


    /** 
     * @brief update lowest ts this node will need to hold
    */
	virtual void UpdateTimestamp(timestamp ts) = 0;

	virtual timestamp GetFrontierTs() const = 0;

    /**
	 * @todo for now don't use it and instead store state for all aggregations
	 * later on we migh switch to this approach
	 * 
     *  @brief if it's stateful node pulls queue of tuples from Table
     * otherwise it pulls them from latest stateful node and recomputes
    virtual Queue *Pull();
     */


    /**
     * @brief returns Queue corresponding to output from this Tuple
     */
	virtual Queue *Output() = 0;
};


/* Source node is responsible for producing data through Compute function and then writing output to both out_queue, and
 * persistent table */
template <typename Type>
class Source : public Node {
public:
	Source(void (*produce)(Type **out_data));

	void Compute() {
		if (this->update_ts_) {
			this->table->UpdateTimestamp(this->ts_);
			this->update_ts_ = false;
		}

		/** @todo produce using this->produce_ */
	}

	Queue<Type> *Output() {
		return this->out_queue_;
	}


	void UpdateTimestamp(timestamp ts) {
		if (ts <= this->ts_) {
			return;
		}
		this->update_ts_ = true;

		if (this->in_node_ != nullptr) {
			this->in_node_->UpdateTimestamp(ts);
		}
	}

    bool Pull(){
        if(this->map_pull_index_ == this->tupl)
    }
	
	timestamp GetFrontierTs() const{
		return this->frontier_ts_;
	}

private:
	bool update_ts_ = false;

    // how much time back from current time do we have to store values
	timestamp frontier_ts_;

    // what is oldest timestamp that needs to be keept by this table
    timestamp ts_;


	void (*produce_)(Type **);

	Queue *out_queue_;
    Queue *produce_queue_;

    // we can treat whole tuple as a key, this will return it's index  
	// we will later use some persistent storage for that mapping, maybe rocksdb or something
    std::unordered_map<char[sizeof(Type)], index> tuple_to_index_;
    // from index we can get list of changes to the input, later
	// we will use some better data structure for that
    std::vector< std::list<Delta>> index_to_deltas_;

};




template <typename Type>
class Filter : public Node {
public:
	Filter(Node *in_node, std::function<bool>(const Type &) > condition)
	    : condition_ {condition}, in_node_ {in_node}, in_queue_ {in_node->Output()} , 
		frontier_ts_{in_node->GetFrontierTs()}
	{
		this->out_queue_ = new Queue(DEFAULT_QUEUE_SIZE, sizeof(Tuple<Type>));
	}

	// filters node
	// those that pass are put into output queue, this is all that this node does
	void Compute() {
		const char *data;
		// pass function that match condition to output
		while (this->in_queue_->GetNext(&data)) {
			const Tuple<Type> *tuple = (const Tuple<Type> *)(data);
			if (this->condition_(tuple->data)) {
				this->out_queue_->Insert(data);
			}
		}
		this->in_queue_->Clean();
	}

	// timestamp is not used here but will be used for global state for propagation
	void UpdateTimestamp(timestamp ts) {
		if (ts <= this->ts_) {
			return;
		}
		if (this->in_node_ != nullptr) {
			this->in_node_->UpdateTimestamp(ts);
		}
	}

private:
	std::function<bool>(const Type *) > condition_;

	// acquired from in node, this will be passed to output node
	timestamp frontier_ts_;
	
	// track this for global timestamp state update
	Node *in_node_;
	timestamp ts_;

	Queue *in_queue_;
	Queue *out_queue_;
};

/** @todo  
 * current approach with delta might not work if for example right table was negative and then becomes positive for except
 * then out delta should be like delete all? wait but not really cause we actually compare with all versions, so older version will show
 * they updates too, definetely something to think about cause for example maybe in out_queue we want to hold single tuple and deltas for each tuple
*/
// projection can be represented by single node
template <typename InType, typename OutType>
class Projection : public Node {
public:
	Projection(Node *in_node,  std::function<OutType(const InType&)>projection)
	    : projection_ {projection}, in_queue_ {in_queue}, in_node_ {in_node}, in_queue_ {in_node->Output()} frontier_ts_{in_node->GetFrontierTs()} 
	{
		this->out_queue_ = new Queue(DEFAULT_QUEUE_SIZE, sizeof(Tuple<Type>));
	}

	void Compute() {
		const char *in_data;
		char *out_data;
		while (in_queue_->GetNext(&in_data)) {
			this->out_queue_->ReserveNext(&out_data);
			Tuple<InType> *in_tuple = (Tuple<InType> *)(in_data);
			Tuple<OutType> *out_tuple = (Tuple<InType> *)(out_data);

			out_tuple->ts = in_tuple->ts;
			out_tuple->count = in_tuple->count;
			out_tuple->data = this->projection_(in_tuple->data);
		}

		this->in_queue_->Clean();
	}

	Queue *Output() const {
		return this->out_queue_;
	}

	// timestamp is not used here but might be helpful to store for propagatio
	void UpdateTimestamp(timestamp ts) {
		if (ts <= this->ts_) {
			return;
		}

		if (this->in_node_ != nullptr) {
			this->in_node_->UpdateTimestamp(ts);
		}
	}

private:
	std::function<OutType(InType)>projection_
	
	// track this for global timestamp state update
	Node *in_node_;
	timestamp ts_;

	// acquired from in node, this will be passed to output node
	timestamp frontier_ts_;
	
	Queue *in_queue_;
	Queue *out_queue_;
};


// this is node behind Union, Except and Intersect, there InType is equal to OutType
template <typename Type>
class SimpleBinaryNode {
public:
	SimpleBinaryNode(Node *in_node_left, Node *in_node_right,
		// it's just using different ways to compute delta for each different kind of node
		std::function<Delta(const Delta &left_delta, const Delta &right_delta)>delta_function;
	):
		in_node_left_{in_node_left}, in_node_right{in_node_right}, 
		in_queue_left_{in_node_left->Output()}, in_queue_right_{in_node_right->Output()},
		delta_function_{delta_function}, this->frontier_ts{std::max(in_node_left_->GetFrontierTs(), in_node_right_->GetFrontierTs())}
	{
		this->ts_ = 0;
		this->out_queue_ = new Queue(DEFAULT_QUEUE_SIZE * 2, sizeof(Tuple<OutType>));
	}

	Queue *Output() {
		return this->out_queue_;
	}

	void Compute() {
		// compute right_queue against left_queue
		// they are small and hold no indexes so we do it just by nested loop
		const char *in_data_left;
		const char *in_data_right;
		char *out_data;
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
			while(this->in_queue_right_->GetNext(&in_data_right)){
				Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
				// if left and right queues match on data put it into out_queue with new delta
				if(std::memcp(in_left_tuple->data, in_right_tuple->data, sizeof(Type))){
					this->out_queue_->ReserveNext(&out_data);
					Delta out_delta = this->delta_function_(in_left_tuple->delta, in_right_tuple->delta);
					Tuple<OutType> *out_tuple = (Tuple<InType> *)(out_data);
					out_tuple->data = in_left_tuple->data;
					out_tuple->delta = out_delta;
				}
			}
		}


		// compute left queue against right table
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
			char left_data* = (char *)&in_left_tuple->data;
			// get all matching on data from right
			index match_index = this->tuple_to_index_right[left_data];
			for(it = this->index_to_deltas_right.begin(); it != this->index_to_deltas_right.end(); it++){
				this->out_queue_->ReserveNext(&out_data);
				Delta out_delta = this->delta_function_(in_left_tuple->delta, *it);
				Tuple<OutType> *out_tuple = (Tuple<InType> *)(out_data);
				out_tuple->data = in_left_tuple->data;
				out_tuple->delta = out_delta;
			}
		}

		// compute right queue against left table
		while (this->in_queue_right_->GetNext(&in_data_right)) {
			Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
			char right_data* = (char *)&in_right_tuple->data;
			// get all matching on data from right
			index match_index = this->tuple_to_index_left[right_data];
			for(it = this->index_to_deltas_left.begin(); it != this->index_to_deltas_left.end(); it++){
				this->out_queue_->ReserveNext(&out_data);
				Delta out_delta = this->delta_function_(*it, in_right_tuple->delta);
				Tuple<OutType> *out_tuple = (Tuple<InType> *)(out_data);
				out_tuple->data = in_right_tuple->data;
				out_tuple->delta = out_delta;
			}
		}

		

		// insert new deltas from in_queues

		// clean in_queues	
		this->in_queue_left_->Clean();
		this->in_queue_right_->Clean();
		
		
		if (this->compact_){
			this->Compact()
		}
	}

	void UpdateTimestamp(timestamp ts) {
		if (ts <= this->ts_) {
			return;
		}
		// way to keep track on when we can get rid of old tuple deltas
		this->ts = ts_;
		if(this->ts_ - this->previous_ts > this->frontier_ts){
			this->compact_ = true;
			this->previous_ts = ts;
		}
		if (this->in_node_left_ != nullptr) {
			this->in_node_left_->UpdateTimestamp(ts);
		}
		if (this->in_node_right_ != nullptr) {
			this->in_node_right_->UpdateTimestamp(ts);
		}
	}

private:
	// timestamp will be used to track valid tuples
	// after update propagate it to input nodes
	bool compact_ = false;
	timestamp ts_;
	timestamp previous_ts;
	
	timestamp frontier_ts_;
	
	Node *in_node_left_;
	Node *in_node_right_;

	Queue *in_queue_left_;
	Queue *in_queue_right_;

	Queue *out_queue_;

	std::function<Delta(const Delta &left_delta, const Delta &right_delta)>delta_function_;

    // we can treat whole tuple as a key, this will return it's index  
	// we will later use some persistent storage for that mapping, maybe rocksdb or something
    std::unordered_map<char[sizeof(Type)], index> tuple_to_index_left;
    std::unordered_map<char[sizeof(Type)], index> tuple_to_index_right;
    // from index we can get list of changes to the input, later
	// we will use some better data structure for that
    std::vector< std::list<Delta>> index_to_deltas_left_;
    std::vector< std::list<Delta>> index_to_deltas_right_;


	std::mutex node_mutex;
};


}
#endif