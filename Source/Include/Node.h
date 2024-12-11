#ifndef ALICEDBNODE
#define ALICEDBNODE

#include "Common.h"
#include "Queue.h"

#include <type_traits>
#include <map>
#include <set>
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
	Source(std::function<void(Type **out_data)>produce) : produce_{produce} {}

	void Compute() {
		if (this->update_ts_) {
			this->table->UpdateTimestamp(this->ts_);
			this->update_ts_ = false;
		}

		/** @todo produce using this->produce_ */
	}

	Queue *Output() {
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

	
	timestamp GetFrontierTs() const{
		return this->frontier_ts_;
	}

private:
	bool update_ts_ = false;

    // how much time back from current time do we have to store values
	timestamp frontier_ts_;

    // what is oldest timestamp that needs to be keept by this table
    timestamp ts_;

	std::function<void(Type **out_data)>produce_;
	
	Queue *out_queue_;
    Queue *produce_queue_;

    // we can treat whole tuple as a key, this will return it's index  
	// we will later use some persistent storage for that mapping, maybe rocksdb or something
    std::unordered_map<char[sizeof(Type)], index> tuple_to_index_;
    // from index we can get list of changes to the input, later
	// we will use some better data structure for that
    std::vector< std::multiset<Delta,bool(*)(const Delta&, const Delta&)>>  index_to_deltas_;

};




template <typename Type>
class Filter : public Node {
public:
	Filter(Node *in_node, std::function<bool(const Type &) > condition)
	    : condition_{condition}, in_node_ {in_node}, in_queue_ {in_node->Output()} , 
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
	std::function<bool(const Type *) > condition_;

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
 * but with our current multiset approach we don't need to do that
*/
// projection can be represented by single node
template <typename InType, typename OutType>
class Projection : public Node {
public:
	Projection(Node *in_node,  std::function<OutType(const InType&)>projection)
	    : projection_ {projection}, in_node_ {in_node}, in_queue_{in_node->Output()}, frontier_ts_{in_node->GetFrontierTs()} 
	{
		this->out_queue_ = new Queue(DEFAULT_QUEUE_SIZE, sizeof(Tuple<OutType>));
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
	std::function<OutType(InType)>projection_;
	
	// track this for global timestamp state update
	Node *in_node_;
	timestamp ts_;

	// acquired from in node, this will be passed to output node
	timestamp frontier_ts_;
	
	Queue *in_queue_;
	Queue *out_queue_;
};

/**
 * @brief this will implement all but Compute functions for Stateful binary nodes
 */

template<typename LeftType, typename RightType, typename OutType>
class StatefulBinaryNode : Node {

	StatefulBinaryNode(Node *in_node_left, Node *in_node_right):
		in_node_left_{in_node_left}, in_node_right_{in_node_right}, 
		in_queue_left_{in_node_left->Output()}, in_queue_right_{in_node_right->Output()},
		frontier_ts_{std::max(in_node_left->GetFrontierTs(), in_node_right->GetFrontierTs())}
	{
		this->ts_ = 0;
		this->out_queue_ = new Queue(DEFAULT_QUEUE_SIZE * 2, sizeof(Tuple<OutType>));
	}

	Queue *Output() {
		return this->out_queue_;
	}

	virtual void Compute() = 0;

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
	void Compact(){
		// Iterate over each multiset in the vector
		for(int index = 0; index < index_to_deltas_left_.size(); index++){
			auto &deltas = index_to_deltas_left_[index];
			int previous_count = 0;
			timestamp ts = 0;

			for (auto it = deltas.rbegin(); it != deltas.rend(); ) {
				previous_count += it->count;
				ts = it->ts;
				auto base_it = std::next(it).base();
				base_it = deltas.erase(base_it);
				it = std::reverse_iterator<decltype(base_it)>(base_it);
				if(it == deltas.rend()) {
					break;
				}
				// Check the condition: ts < ts_ - frontier_ts_
				if(it->ts < (ts_ - frontier_ts_)){
					continue;
				}
				else{
					deltas.insert(Delta{ts, previous_count});
					break;
				}
			}
		}

		// Iterate over each multiset in the vector
		for(int index = 0; index < index_to_deltas_right_.size(); index++){
			auto &deltas = index_to_deltas_right_[index];
			int previous_count = 0;
			timestamp ts = 0;

			for (auto it = deltas.rbegin(); it != deltas.rend(); ) {
				previous_count += it->count;
				ts = it->ts;
				auto base_it = std::next(it).base();
				base_it = deltas.erase(base_it);
				it = std::reverse_iterator<decltype(base_it)>(base_it);
				if(it == deltas.rend()) {
					break;
				}
				// Check the condition: ts < ts_ - frontier_ts_
				if(it->ts < (ts_ - frontier_ts_)){
					continue;
				}
				else{
					deltas.insert(Delta{ts, previous_count});
					break;
				}
			}
		}

	}

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

	size_t next_index_left_=0;
	size_t next_index_right_=0;


    // we can treat whole tuple as a key, this will return it's index  
	// we will later use some persistent storage for that mapping, maybe rocksdb or something
    std::unordered_map<char[sizeof(LeftType)], index> tuple_to_index_left;
    std::unordered_map<char[sizeof(RightType)], index> tuple_to_index_right;
    // from index we can get multiset of changes to the input, later
	// we will use some better data structure for that, but for now multiset is nice cause it auto sorts for us
    std::vector< std::multiset<Delta,bool(*)(const Delta&, const Delta&)>>  index_to_deltas_left_;
    std::vector< std::multiset<Delta,bool(*)(const Delta&, const Delta&)>>  index_to_deltas_right_;

	std::mutex node_mutex;
};


// this is node behind Union, Except and Intersect, there InType is equal to OutType
/** @todo it needs better name :) */
template <typename Type>
class SimpleBinaryNode: public StatefulBinaryNode<Type, Type, Type> {
public:
	SimpleBinaryNode(Node *in_node_left, Node *in_node_right,
		// it's just using different ways to compute delta for each different kind of node
		std::function<Delta(const Delta &left_delta, const Delta &right_delta)>delta_function
	): StatefulBinaryNode<Type, Type, Type>(in_node_left, in_node_right),
		delta_function_{delta_function}
	{}

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
				if(std::memcmp(in_left_tuple->data, in_right_tuple->data, sizeof(Type))){
					this->out_queue_->ReserveNext(&out_data);
					Delta out_delta = this->delta_function_(in_left_tuple->delta, in_right_tuple->delta);
					Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
					out_tuple->data = in_left_tuple->data;
					out_tuple->delta = out_delta;
				}
			}
		}


		// compute left queue against right table
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
			char *left_data = (char *)&in_left_tuple->data;
			// get all matching on data from right
			index match_index = this->tuple_to_index_right[left_data];
			for(auto it = this->index_to_deltas_right[match_index].begin(); it != this->index_to_deltas_right[match_index].end(); it++){
				this->out_queue_->ReserveNext(&out_data);
				Delta out_delta = this->delta_function_(in_left_tuple->delta, *it);
				Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
				out_tuple->data = in_left_tuple->data;
				out_tuple->delta = out_delta;
			}
		}

		// compute right queue against left table
		while (this->in_queue_right_->GetNext(&in_data_right)) {
			Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
			char *right_data = (char *)&in_right_tuple->data;
			// get all matching on data from right
			index match_index = this->tuple_to_index_left[right_data];
			for(auto it = this->index_to_deltas_left[match_index].begin(); it != this->index_to_deltas_left[match_index].end(); it++){
				this->out_queue_->ReserveNext(&out_data);
				Delta out_delta = this->delta_function_(*it, in_right_tuple->delta);
				Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
				out_tuple->data = in_right_tuple->data;
				out_tuple->delta = out_delta;
			}
		}

		
		// insert new deltas from in_queues
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
			// if this data wasn't present insert with new index
			if(!this->tuple_to_index_left.contains(in_left_tuple->data)){
				index match_index = this->next_index_left_;
				this->next_index_left_++;
				this->tuple_to_index_left[in_left_tuple->data] = match_index;
				this->index_to_deltas_left.emplace_back({in_left_tuple->delta});
			}
			else{
				index match_index = this->tuple_to_index_left[&in_left_tuple->data];
				this->index_to_deltas_left[match_index].insert(in_left_tuple->delta);
			}
		}
		while (this->in_queue_right_->GetNext(&in_data_right)) {
			Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
			// if this data wasn't present insert with new index
			if(!this->tuple_to_index_right.contains(in_right_tuple->data)){
				index match_index = this->next_index_right_;
				this->next_index_right_++;
				this->tuple_to_index_right[in_right_tuple->data] = match_index;
				this->index_to_deltas_right.emplace_back({in_right_tuple->delta});
			}
			else{
				index match_index = this->tuple_to_index_left[&in_right_tuple->data];
				this->index_to_deltas_left[match_index].insert(in_right_tuple->delta);
			}
		}
		
		// clean in_queues	
		this->in_queue_left_->Clean();
		this->in_queue_right_->Clean();
		
		
		if (this->compact_){
			this->Compact();
		}
	}
	// extra state beyond StatefulNode is only deltafunction
	std::function<Delta(const Delta &left_delta, const Delta &right_delta)>delta_function_;
};

template <typename T>
class Union: SimpleBinaryNode<T>{
	Union(Node *in_node_left, Node *in_node_right): SimpleBinaryNode<T>{in_node_left, in_node_right, delta_function}
	{}
	static Delta delta_function(const Delta &left_delta, const Delta &right_delta){
		return {std::max(left_delta.ts, right_delta.ts), left_delta.count + right_delta.count};
	}
};
template <typename T>
class Intersect: SimpleBinaryNode<T>{
	Intersect(Node *in_node_left, Node *in_node_right): SimpleBinaryNode<T>{in_node_left, in_node_right, delta_function}
	{}
	static Delta delta_function(const Delta &left_delta, const Delta &right_delta){
		return {std::max(left_delta.ts, right_delta.ts), left_delta.count - right_delta.count};
	}
};
template <typename T>
class Except: SimpleBinaryNode<T>{
	Except(Node *in_node_left, Node *in_node_right): SimpleBinaryNode<T>{in_node_left, in_node_right, delta_function}
	{}
	static Delta delta_function(const Delta &left_delta, const Delta &right_delta){
		return {std::max(left_delta.ts, right_delta.ts), left_delta.count * right_delta.count};
	}
};

template <typename InTypeLeft, typename InTypeRight, typename OutType>
class CrossJoinNode: public StatefulBinaryNode<InTypeLeft, InTypeRight, OutType> {
public:
	CrossJoinNode(Node *in_node_left, Node *in_node_right, std::function<OutType(InTypeLeft,InTypeRight)>join_layout):
	 	StatefulBinaryNode<InTypeLeft, InTypeRight, OutType>(in_node_left, in_node_right),
	 	join_layout_{join_layout} {}

	void Compute() {

		// compute right_queue against left_queue
		// they are small and hold no indexes so we do it just by nested loop
		const char *in_data_left;
		const char *in_data_right;
		char *out_data;
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			while(this->in_queue_right_->GetNext(&in_data_right)){
				Tuple<InTypeRight> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
				// if left and right queues match on data put it into out_queue with new delta
				this->out_queue_->ReserveNext(&out_data);
				Tuple<OutType> *out_tuple = (Tuple<OutType>*)(out_data);
				out_tuple->delta = {std::max(in_left_tuple->delta.ts, in_right_tuple->delta.ts),in_left_tuple->delta.count * in_right_tuple->delta.count};
				&out_tuple->data = this->join_layout_(in_left_tuple, in_right_tuple);
			}
		}

		// compute left queue against right table
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			char *left_data = (char *)&in_left_tuple->data;
			// get all matching on data from right
			index match_index = this->tuple_to_index_right[left_data];
			for(auto &[table_data, table_idx] : this->tuple_to_index_right){
				for(auto it = this->index_to_deltas_right[table_idx].begin(); it != this->index_to_deltas_right[table_idx].end(); it++){
					this->out_queue_->ReserveNext(&out_data);
					Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
					out_tuple->delta = {std::max(in_left_tuple->delta.ts, it->ts),in_left_tuple->delta.count * it->count};
					out_tuple->data =  this->join_layout_(in_left_tuple->data, table_data);
				}
			}
		}

		// compute right queue against left table
		while (this->in_queue_right_->GetNext(&in_data_right)) {
			Tuple<InTypeRight> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
			char *right_data = (char *)&in_right_tuple->data;
			// get all matching on data from right
			index match_index = this->tuple_to_index_left[right_data];
			for(auto &[ table_data, table_idx] : this->tuple_to_index_right){
				for(auto it = this->index_to_deltas_left[table_idx].begin(); it != this->index_to_deltas_left[table_idx].end(); it++){
					this->out_queue_->ReserveNext(&out_data);
					Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
					out_tuple->delta = {std::max(in_right_tuple->delta.ts, it->ts),in_right_tuple->delta.count * it->count};
					out_tuple->data =  this->join_layout_(table_data, in_right_tuple->data);
				}
			}
		}


		// insert new deltas from in_queues
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			// if this data wasn't present insert with new index
			if(!this->tuple_to_index_left.contains(in_left_tuple->data)){
				index match_index = this->next_index_left_;
				this->next_index_left_++;
				this->tuple_to_index_left[in_left_tuple->data] = match_index;
				this->index_to_deltas_left.emplace_back({in_left_tuple->delta});
			}
			else{
				index match_index = this->tuple_to_index_left[&in_left_tuple->data];
				this->index_to_deltas_left[match_index].insert(in_left_tuple->delta);
			}
		}

		while (this->in_queue_right_->GetNext(&in_data_right)) {
			Tuple<InTypeRight> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
			// if this data wasn't present insert with new index
			if(!this->tuple_to_index_right.contains(in_right_tuple->data)){
				index match_index = this->next_index_right_;
				this->next_index_right_++;
				this->tuple_to_index_right[in_right_tuple->data] = match_index;
				this->index_to_deltas_right.emplace_back({in_right_tuple->delta});
			}
			else{
				index match_index = this->tuple_to_index_left[&in_right_tuple->data];
				this->index_to_deltas_left[match_index].insert(in_right_tuple->delta);
			}
		}
		
		// clean in_queues	
		this->in_queue_left_->Clean();
		this->in_queue_right_->Clean();
		
		
		if (this->compact_){
			this->Compact();
		}
	}
private:

	// only state beyound StatefulNode state is join_layout_function
	std::function<OutType(InTypeLeft,InTypeRight)>join_layout_;  
};



// join on
// match type could be even char[] in this case
template <typename InTypeLeft, typename InTypeRight, typename MatchType , typename OutType>
class JoinNode: public StatefulBinaryNode<InTypeLeft, InTypeRight, OutType> {
public:
	JoinNode(Node *in_node_left, Node *in_node_right, 
		std::function<MatchType(InTypeLeft *)>get_match_left,
		std::function<MatchType(InTypeRight*)>get_match_right,
		std::function<OutType(InTypeLeft*, InTypeRight *)>join_layout):
	StatefulBinaryNode<InTypeLeft, InTypeRight, OutType>(in_node_left, in_node_right),
	get_match_left_(get_match_left), get_match_right_{get_match_right}, join_layout_{join_layout}
	{}


	// this function changes
	void Compute() {

		// compute right_queue against left_queue
		// they are small and hold no indexes so we do it just by nested loop
		const char *in_data_left;
		const char *in_data_right;
		char *out_data;
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			while(this->in_queue_right_->GetNext(&in_data_right)){
				Tuple<InTypeRight> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
				// if left and right queues match on data put it into out_queue with new delta
				if (this->Compare(&in_left_tuple->data, &in_right_tuple->data) ){
					this->out_queue_->ReserveNext(&out_data);
					Tuple<OutType> *out_tuple = (Tuple<OutType>*)(out_data);
					out_tuple->delta = {std::max(in_left_tuple->delta.ts, in_right_tuple->delta.ts),in_left_tuple->delta.count * in_right_tuple->delta.count};
					&out_tuple->data = this->join_layout_(in_left_tuple, in_right_tuple);
				}
			}
		}

		// compute left queue against right table
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			// get all matching on data from right
			MatchType *match = this->get_match_left_(in_left_tuple->data);
			// all tuples from right table that match this left tuple
			for(auto tpl_it = this->match_to_tuple_right_[match].begin(); tpl_it != this->match_to_tuple_right_.end(); tpl_it++){
				// now iterate all version of this tuple
				char (&right_data)[sizeof(InTypeRight)] = *tpl_it;
				for(auto it = this->index_to_deltas_right[right_data].begin(); it != this->index_to_deltas_right[right_data].end(); it++){
					this->out_queue_->ReserveNext(&out_data);
					Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
					out_tuple->delta = {std::max(in_left_tuple->delta.ts, it->ts),in_left_tuple->delta.count * it->count};
					out_tuple->data =  this->join_layout_(in_left_tuple->data, right_data);
				}
			}
		}
		// compute left queue against right table
		while (this->in_queue_right_->GetNext(&in_data_right)) {
			Tuple<InTypeLeft> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
			// get all matching on data from right
			MatchType *match = this->get_match_right_(in_right_tuple->data);
			// all tuples from right table that match this left tuple
			for(auto tpl_it = this->match_to_tuple_left_[match].begin(); tpl_it != this->match_to_tuple_left_.end(); tpl_it++){
				// now iterate all version of this tuple
				char (&left_data)[sizeof(InTypeLeft)] = *tpl_it;
				for(auto it = this->index_to_deltas_left[left_data].begin(); it != this->index_to_deltas_left[left_data].end(); it++){
					this->out_queue_->ReserveNext(&out_data);
					Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
					out_tuple->delta = {std::max(in_right_tuple->delta.ts, it->ts),in_right_tuple->delta.count * it->count};
					out_tuple->data =  this->join_layout_(left_data, &in_right_tuple->data);
				}
			}
		}


		// insert new deltas from in_queues
		while (this->in_queue_left_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			// if this data wasn't present insert with new index
			if(!this->tuple_to_index_left.contains(in_left_tuple->data)){
				index match_index = this->next_index_left_;
				this->next_index_left_++;
				this->tuple_to_index_left[in_left_tuple->data] = match_index;
				this->index_to_deltas_left.emplace_back({in_left_tuple->delta});
			}
			else{
				index match_index = this->tuple_to_index_left[&in_left_tuple->data];
				this->index_to_deltas_left[match_index].insert(in_left_tuple->delta);
			}
		}

		while (this->in_queue_right_->GetNext(&in_data_right)) {
			Tuple<InTypeRight> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
			// if this data wasn't present insert with new index
			if(!this->tuple_to_index_right.contains(in_right_tuple->data)){
				index match_index = this->next_index_right_;
				this->next_index_right_++;
				this->tuple_to_index_right[in_right_tuple->data] = match_index;
				this->index_to_deltas_right.emplace_back({in_right_tuple->delta});
			}
			else{
				index match_index = this->tuple_to_index_left[&in_right_tuple->data];
				this->index_to_deltas_left[match_index].insert(in_right_tuple->delta);
			}
		}
		
		// clean in_queues	
		this->in_queue_left_->Clean();
		this->in_queue_right_->Clean();
		
		
		if (this->compact_){
			this->Compact();
		}
	}

private:

	inline bool Compare(InTypeLeft *left_data, InTypeRight *right_data) { 
			return std::memcmp(this->get_match_left(left_data), this->get_match_right(right_data), sizeof(MatchType));
	}

	// we need those functions to calculate matchfields from tuples on left and righ
	std::function<MatchType(InTypeLeft *)>get_match_left_;
	std::function<MatchType(InTypeRight*)>get_match_right_;	
	std::function<OutType(InTypeLeft,InTypeRight)>join_layout_;
  
	// we need to get corresponding tuples using only match chars, this maps will help us with it
    std::unordered_map<char[sizeof(MatchType)], std::list<char[sizeof(InTypeLeft)]> > match_to_tuple_left_;
    std::unordered_map<char[sizeof(MatchType)], std::list<char[sizeof(InTypeLeft)]> > match_to_tuple_right_;

};


/**
 * OK for this node we might want to use bit different model of computation
 * it will only emit single value at right time, and it need to delete old value,
 * it also need to compute next value based not only on data but also on delta
 */


/** @todo do we really need to keep and send to out_queue this initial value? maybe delta would be enough */
// aggregations, is only Unary stateful node and there are two kinds of aggregates, normal and aggregate by
// lets start with simple aggregate
template <typename InType, typename OutType>
class AggregateNode: Node{
	// now output of aggr might be also dependent of count of in tuple, for example i sum inserting 5 Alice's should influence it different than one Alice
	// but with min it should not, so it will be dependent on aggregate function
	AggregateNode(Node *in_node, std::function<OutType(OutType, InType, int count)> aggr_func, OutType initial_value):
	in_node_{in_node},
	in_queue_{in_node->Output()},
	frontier_ts_{in_node->GetFrontierTs()},
	initial_value_{initial_value},
	aggr_func_{aggr_func}
	{
		this->ts_ = 0;
		// there will be single tuple emited at once probably, if not it will get resized so chill
		this->out_queue_ = new Queue( 2, sizeof(Tuple<OutType>));
	}

	Queue *Output() {
		return this->out_queue_;
	}

	// output should be single tuple with updated values for different times
	// so what we can do there? if we emit new count as part of value, then it will be treated as separate tuple
	// if we emit new value as part of delta it also will be wrong
	// what if we would do : insert previous value with count -1 and insert current with count 1 at the same time
	// then old should get discarded
	void Compute() {

    	std::multiset<Delta,bool(*)(const Delta&, const Delta&)>  new_deltas;
		const char *in_data;
		while (this->in_queue_left_->GetNext(&in_data)) {
				Tuple<InType> *in_tuple = (Tuple<InType> *)(in_data);
				// compute against all version of current value
				for(auto &[ts, count] : this->deltas_){
					new_deltas.insert({std::max(ts, in_tuple->delta.ts), aggr_function(count, in_tuple->data, in_tuple->delta.count)});
				}	
		}

		// merge deltas
		this->deltas_.insert(new_deltas.begin(), new_deltas.end());

		auto oldest = deltas_.end()--;
		// emit only single one  with right timestamp
		for (auto it = deltas_.rbegin(); it != deltas_.rend(); ) {
			// if this is first to new to be emited try emiting previous one
			if (it->ts > this->ts_ - this->frontier_ts_){
				auto prev = it--;
				if(it != deltas_.rbegin()){

					char *out_data;
					this->out_queue_->ReserveNext(&out_data);
					char *del_data;
					this->out_queue_->ReserveNext(&del_data);

					// emit new
					Tuple<OutType> out_tuple = (Tuple<OutType> *)(out_data);
					out_tuple.data = prev->count;
					out_tuple.delta = {prev->ts, 1};

					// delete oldest
					Tuple<OutType> del_tuple = (Tuple<OutType> *)(del_data) = {oldest->ts, -1, oldest->count};

					// compact
					this->Compact();
				}
				
				break;
			}
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
		if (this->in_node_ != nullptr) {
			this->in_node_->UpdateTimestamp(ts);
		}
	}

private:
	// let's store full version data and not deltas in this node, so we can just discard old versions
	void Compact(){
		// Iterate over each multiset in the vector
		for (auto it = deltas_.rbegin(); it != deltas_.rend(); it++ ) {
			if (it->ts <= this->previous_ts){
				auto base_it = std::next(it).base();
				base_it = deltas_.erase(base_it);
				it = std::reverse_iterator<decltype(base_it)>(base_it);
			}
			else {
				break;
			}
		}
		this->compact = false;
	}

	// timestamp will be used to track valid tuples
	// after update propagate it to input nodes
	bool compact_ = false;
	timestamp ts_;
	timestamp previous_ts;
	
	timestamp frontier_ts_;
	
	Node *in_node_;

	Queue *in_queue_;

	Queue *out_queue_;

	// there will be only single out tuple with multiple version, so we don't need indexing
    std::multiset<Delta,bool(*)(const Delta&, const Delta&)>  deltas_;
 
 	std::function<OutType(OutType, InType, int)> aggr_func_;
	OutType initial_value_;
	
	std::mutex node_mutex;

};

// and finally aggregate_by, so we are aggregating but also with grouping by fields


}
#endif