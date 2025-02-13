/** 
 *
 * ok we allow for 1 but without allowing for dynamically resizing graph, instead it cannot
 * change after start command;
 *
 * for nodes that needs match fields store in rocks db <Key: match_fields| data
 *><Value Index> for normal ones <Key: data ><Value: Index>
 *
 * we also store <index, ts> <count> in separate storage to calculate deltas
 */

#ifndef ALICEDBNODE
#define ALICEDBNODE

#include "Common.h"
#include "EdgeCache.h"
#include "Producer.h"
#include "Storage.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <type_traits>

template <typename Type>
std::array<char, sizeof(Type)> Key(const Type &type) {
	std::array<char, sizeof(Type)> key;
	std::memcpy(key.data(), &type, sizeof(Type));
	return key;
}
template <typename Type>
struct KeyHash {
	std::size_t operator()(const std::array<char, sizeof(Type)> &key) const {
		// You can use any suitable hash algorithm. Here, we'll use std::hash with
		// std::string_view
		return std::hash<std::string_view>()(std::string_view(key.data(), key.size()));
	}
};

#define DEFAULT_CACHE_SIZE (200)

namespace AliceDB {

// we need this definition to store graph pointer in node
class Graph;
class BufferPool;

class Node {
public:
	virtual ~Node() {};

	virtual void Compute() = 0;

	virtual void CleanCache() = 0;
	/**
	 * @brief update lowest ts this node will need to hold
	 */
	virtual void UpdateTimestamp(timestamp ts) = 0;

	virtual timestamp GetFrontierTs() const = 0;

	/**
	 * @brief returns cache corresponding to output from this Tuple
	 */
	virtual Cache *Output() = 0;
};

template <typename OutType>
class TypedNode : public Node {
public:
	using value_type = OutType;

	virtual ~TypedNode() {};
};

/* Source node is responsible for producing data through Compute function and
 * then writing output to both out_cache, and persistent table creator of this
 * node needs to specify how long delayed data might arrive
 */
template <typename Type>
class SourceNode : public TypedNode<Type> {
public:
	SourceNode(Producer<Type> *prod, timestamp frontier_ts, int duration_us, Graph *graph)
	    : produce_ {prod}, frontier_ts_ {frontier_ts}, duration_us_ {duration_us}, graph_ {graph} {
		this->produce_cache_ = new Cache(DEFAULT_CACHE_SIZE, sizeof(Tuple<Type>));
		this->ts_ = get_current_timestamp();
	}

	~SourceNode() {
		delete produce_cache_;
	}

	void Compute() {
		auto start = std::chrono::steady_clock::now();
		std::chrono::microseconds duration(this->duration_us_);
		auto end = start + duration;
		int cnt = 0;
		char *prod_data;
		// produce some data with time limit set, into produce_cache
		while (std::chrono::steady_clock::now() < end) {
			produce_cache_->ReserveNext(&prod_data);
			Tuple<Type> *prod_tuple = (Tuple<Type> *)(prod_data);
			prod_tuple->delta.ts = get_current_timestamp();
			if (!this->produce_->next(prod_tuple)) {
				// we reserved but won't insert so have to remove it from cache
				produce_cache_->RemoveLast();
				break;
			}
			cnt++;
		}
	}

	Cache *Output() {
		this->out_count++;
		return this->produce_cache_;
	}

	void CleanCache() {
		clean_count++;
		if (this->clean_count == this->out_count) {
			this->produce_cache_->Clean();
			this->clean_count = 0;
		}
	}

	void UpdateTimestamp(timestamp ts) {
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->ts_ = ts;
		}
		return;
	}

	timestamp GetFrontierTs() const {
		return this->frontier_ts_;
	}

private:
	int duration_us_;

	bool update_ts_ = false;

	// how much time back from current time do we have to store values
	timestamp frontier_ts_;

	// what is oldest timestamp that needs to be keept by this table
	timestamp ts_;

	Producer<Type> *produce_;

	Graph *graph_;

	// data from producer is put into this cache from it it's written into both
	// table and passed to output nodes
	Cache *produce_cache_;
	int out_count = 0;
	int clean_count = 0;
};

template <typename Type>
class SinkNode : public TypedNode<Type> {
public:
	SinkNode(TypedNode<Type> *in_node, Graph *graph, BufferPool *bp, index table_index)
	    : in_node_(in_node), in_cache_ {in_node->Output()}, frontier_ts_ {in_node->GetFrontierTs()}, graph_ {graph} {
		this->ts_ = get_current_timestamp();

		// init table from graph metastate based on index

		// get reference to corresponding metastate
		MetaState &meta = this->graph_->tables_metadata_(table_index);

		this->table_ = new Table<Type>(meta.delta_filename_, meta.pages_, meta.btree_pages_, bp, graph_);

		// we also need to set ts for the node
		ts_ = meta.ts_;
	}

	~SinkNode() {
		delete table_;
	}

	void Compute() {
		// write in cache into out_table
		const char *in_data;
		while (this->in_cache_->GetNext(&in_data)) {
			Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data);
			index idx = this->table_->Insert(in_tuple->data);
			this->table_->InsertDelta(idx, in_tuple->delta);
		}

		// get current timestamp that can be considered
		this->UpdateTimestamp(get_current_timestamp());

		if (this->compact_) {
			this->Compact();
			this->update_ts_ = false;
		}
		this->in_node_->CleanCache();
	}

	// since all sink does is store state we can treat incache as out cache when we use sink(view)
	// as source
	Cache *Output() {
		return this->in_node_->Output();
	}
	void CleanCache() {
		this->in_node_->CleanCache();
	}

	void UpdateTimestamp(timestamp ts) {
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->update_ts_ = true;
			this->ts_ = ts;
			this->in_node_->UpdateTimestamp(ts);
		}
	}

	timestamp GetFrontierTs() const {
		return this->frontier_ts_;
	}

private:
	void Compact() {
		this->table_->MergeDelta(this->ts_);
	}

	size_t next_index_ = 0;
	bool compact_;

	bool update_ts_ = false;

	// how much time back from current time do we have to store values
	timestamp frontier_ts_;

	// what is oldest timestamp that needs to be keept by this Node Tables
	timestamp &ts_;

	TypedNode<Type> *in_node_;
	// in cache is out cache :)
	Cache *in_cache_;

	// we can treat whole tuple as a key, this will return it's index
	// we will later use some persistent storage for that mapping, maybe rocksdb
	// or something

	Table<Type> *table_;

	Graph *graph_;
};

template <typename Type>
class FilterNode : public TypedNode<Type> {
public:
	FilterNode(TypedNode<Type> *in_node, std::function<bool(const Type &)> condition, Graph *graph)
	    : condition_ {condition}, in_node_ {in_node}, in_cache_ {in_node->Output()},
	      frontier_ts_ {in_node->GetFrontierTs()}, graph_ {graph} {
		this->out_cache_ = new Cache(DEFAULT_CACHE_SIZE, sizeof(Tuple<Type>));
		this->ts_ = get_current_timestamp();
	}

	~FilterNode() {
		delete out_cache_;
	}

	// filters node
	// those that pass are put into output cache, this is all that this node does
	void Compute() {
		const char *data;
		// pass function that match condition to output
		while (this->in_cache_->GetNext(&data)) {
			const Tuple<Type> *tuple = (const Tuple<Type> *)(data);
			if (this->condition_(tuple->data)) {
				this->out_cache_->Insert(data);
			}
		}
		this->in_node_->CleanCache();
	}

	Cache *Output() {
		this->out_count++;
		return this->out_cache_;
	}
	void CleanCache() {
		clean_count++;
		if (this->clean_count == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count = 0;
		}
	}

	timestamp GetFrontierTs() const {
		return this->frontier_ts_;
	}

	// timestamp is not used here but will be used for global state for
	// propagation
	void UpdateTimestamp(timestamp ts) {
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->ts_ = ts;
			this->in_node_->UpdateTimestamp(ts);
		}
	}

private:
	std::function<bool(const Type &)> condition_;

	// acquired from in node, this will be passed to output node
	timestamp frontier_ts_;

	// track this for global timestamp state update
	TypedNode<Type> *in_node_;
	timestamp ts_;

	Cache *in_cache_;
	Cache *out_cache_;
	int out_count = 0;
	int clean_count = 0;

	Graph *graph_;
};
// projection can be represented by single node
template <typename InType, typename OutType>
class ProjectionNode : public TypedNode<OutType> {
public:
	ProjectionNode(TypedNode<InType> *in_node, std::function<OutType(const InType &)> projection, Graph *graph)
	    : projection_ {projection}, in_node_ {in_node}, in_cache_ {in_node->Output()},
	      frontier_ts_ {in_node->GetFrontierTs()}, graph_ {graph} {
		this->out_cache_ = new Cache(DEFAULT_CACHE_SIZE, sizeof(Tuple<OutType>));
		this->ts_ = get_current_timestamp();
	}

	~ProjectionNode() {
		delete out_cache_;
	}

	void Compute() {
		const char *in_data;
		char *out_data;
		while (in_cache_->GetNext(&in_data)) {
			this->out_cache_->ReserveNext(&out_data);
			Tuple<InType> *in_tuple = (Tuple<InType> *)(in_data);
			Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);

			out_tuple->delta.ts = in_tuple->delta.ts;
			out_tuple->delta.count = in_tuple->delta.count;
			out_tuple->data = this->projection_(in_tuple->data);
		}

		this->in_node_->CleanCache();
	}

	Cache *Output() {
		this->out_count++;
		return this->out_cache_;
	}
	void CleanCache() {
		clean_count++;
		if (this->clean_count == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count = 0;
		}
	}

	timestamp GetFrontierTs() const {
		return this->frontier_ts_;
	}

	// timestamp is not used here but might be helpful to store for propagatio
	void UpdateTimestamp(timestamp ts) {
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->ts_ = ts;
			this->in_node_->UpdateTimestamp(ts);
		}
	}

private:
	std::function<OutType(const InType &)> projection_;

	// track this for global timestamp state update
	TypedNode<InType> *in_node_;
	timestamp ts_;

	// acquired from in node, this will be passed to output node
	timestamp frontier_ts_;

	Cache *in_cache_;
	Cache *out_cache_;
	int out_count = 0;
	int clean_count = 0;

	Graph *graph_;
};

// we need distinct node that will:
// for tuples that have positive count -> produce tuple with count 1 if positive
// for tuples that have negative count -> produce tuple with count 0 otherwise

// but the catch is: what this node will emit depends fully on previouse state:
/*
        if previous state was 0
                if now positive emit 1
                if now negative don't emit
        if previouse state was 1
                if now positive don't emit
                if now negative emit -1
        if there was no previous state:
          if now positive emit 1
*/
template <typename Type>
class DistinctNode : public TypedNode<Type> {
public:
	DistinctNode(TypedNode<Type> *in_node, Graph *graph, BufferPool *bp, index table_index)
	    : in_cache_ {in_node->Output()}, in_node_ {in_node}, frontier_ts_ {in_node->GetFrontierTs()}, graph_ {graph} {
		this->ts_ = get_current_timestamp();
		this->previous_ts_ = 0;
		this->out_cache_ = new Cache(DEFAULT_CACHE_SIZE, sizeof(Tuple<Type>));

		// init table from graph metastate based on index

		// get reference to corresponding metastate
		MetaState &meta = this->graph_->tables_metadata_(table_index);

		this->table_ = new Table<Type>(meta.delta_filename_, meta.pages_, meta.btree_pages_, bp, graph_);

		// we also need to set ts for the node
		ts_ = meta.ts_;
	}

	~DistinctNode() {
		delete out_cache_;
		delete table_;
	}

	Cache *Output() {
		this->out_count++;
		return this->out_cache_;
	}
	void CleanCache() {
		clean_count++;
		if (this->clean_count == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count = 0;
		}
	}

	timestamp GetFrontierTs() const {
		return this->frontier_ts_;
	}

	void Compute() {
		// whether tuple for given index was ever emited, this is needed for compute state machine
		std::unordered_set<index> not_emited;

		// first insert all new data from cache to table
		const char *in_data_;
		while (this->in_cache_->GetNext(&in_data_)) {
			const Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data_);

			index idx = this->table_->Insert(in_tuple->data);
			bool was_present = this->table_->InsertDelta(idx, in_tuple->delta);

			if (!was_present) {
				not_emited.insert(idx);
			}
		}

		// then if compact_
		if (this->compact_) {
			// delta_count for oldest keept verson fro this we can deduce what tuples
			// to emit;
			std::vector<Delta> oldest_deltas_ {this->table_->DeltasSize()};

			// emit delete for oldest keept version, emit insert for previous_ts

			// iterate by tuple to index
			for (size_t index = 0; index < this->table_->DeltasSize(); index++) {
				oldest_deltas_[index] = this->table_->OldestDelta(index);
			}

			this->Compact();
			this->compact_ = false;

			// use heap iterator to go through all tuples
			index idx = 0;
			for (auto it = this->table_->HeapIterator.begin(); it != this->table_->HeapIterator.end();
			     ++it, idx++) {
				// iterate by delta tuple, ok since tuples are appeneded sequentially we can get index from tuple
				// position using heap iterator, this should be fast since distinct shouldn't store that many tuples
				Delta cur_delta = this->table_->OldestDelta[idx];
				bool previous_positive = oldest_deltas_[idx].count > 0;
				bool current_positive = cur_delta.count > 0;
				/*
				        Now we can deduce what to emit based on this index value from
				   oldest_delta, negative delta -> previouse state was 0 positive delta
				   -> previouse state was 1

				        if previous state was 0
				                if now positive emit 1
				                if now negative don't emit
				        if previouse state was 1
				                if now positive don't emit
				                if now negative emit -1

				*/

				char *out_data;

				// but if it's first iteration of this Node we need to always emit
				if (not_emited.contains(idx)) {
					if (current_positive) {
						this->out_cache_->ReserveNext(&out_data);
						Tuple<Type> *update_tpl = (Tuple<Type> *)(out_data);
						update_tpl->delta.ts = cur_delta.ts;
						update_tpl->delta.count = 1;
						// finally copy data
						std::memcpy(&update_tpl->data, *it, sizeof(Type));
					}

				} else {
					if (previous_positive) {
						if (current_positive) {
							continue;
						} else {
							this->out_cache_->ReserveNext(&out_data);
							Tuple<Type> *update_tpl = (Tuple<Type> *)(out_data);
							update_tpl->delta.ts = cur_delta.ts;
							update_tpl->delta.count = -1;
							// finally copy data
							std::memcpy(&update_tpl->data, *it, sizeof(Type));
						}
					} else {
						if (current_positive) {
							this->out_cache_->ReserveNext(&out_data);
							Tuple<Type> *update_tpl = (Tuple<Type> *)(out_data);
							update_tpl->delta.ts = cur_delta.ts;
							update_tpl->delta.count = 1;
							// finally copy data
							std::memcpy(&update_tpl->data, *it, sizeof(Type));
						} else {
							continue;
						}
					}
				}
			}
		}
	}

	void UpdateTimestamp(timestamp ts) {
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->compact_ = true;
			this->previous_ts_ = this->ts_;
			this->ts_ = ts;
			this->in_node_->UpdateTimestamp(ts);
		}
	}

private:
	void Compact() {
		// leave previous_ts version as oldest one
		this->table_->MergeDelta(this->previous_ts_);
	}

	// timestamp will be used to track valid tuples
	// after update propagate it to input nodes
	bool compact_ = false;
	timestamp &ts_;

	timestamp previous_ts_ = 0;

	timestamp frontier_ts_;

	TypedNode<Type> *in_node_;

	Cache *in_cache_;

	Cache *out_cache_;
	int out_count = 0;
	int clean_count = 0;

	// we can treat whole tuple as a key, this will return it's index
	// we will later use some persistent storage for that mapping, maybe rocksdb
	// or something

	Table<Type> *table_;
	Graph *graph_;
};

// stateless binary operator, combines data from both in caches and writes them
// to out_cache
/*
        Union is PlusNode -> DistinctNode
        Except is plus with NegateLeft -> DistinctNode
*/
template <typename Type>
class PlusNode : public TypedNode<Type> {
public:
	// we can make it subtractor by setting neg left to true, then left deltas are
	// reversed
	PlusNode(TypedNode<Type> *in_node_left, TypedNode<Type> *in_node_right, bool negate_left)
	    : negate_left_(negate_left), in_node_left_ {in_node_left}, in_node_right_ {in_node_right},
	      in_cache_left_ {in_node_left->Output()}, in_cache_right_ {in_node_right->Output()},
	      frontier_ts_ {std::max(in_node_left->GetFrontierTs(), in_node_right->GetFrontierTs())} {
		this->ts_ = get_current_timestamp();
		this->out_cache_ = new Cache(DEFAULT_CACHE_SIZE, sizeof(Tuple<Type>));
	}

	~PlusNode() {
		delete out_cache_;
	}

	Cache *Output() {
		this->out_count++;
		return this->out_cache_;
	}
	void CleanCache() {
		clean_count++;
		if (this->clean_count == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count = 0;
		}
	}

	timestamp GetFrontierTs() const {
		return this->frontier_ts_;
	}

	void Compute() {
		// process left input
		const char *in_data;
		char *out_data;
		while (in_cache_left_->GetNext(&in_data)) {
			this->out_cache_->ReserveNext(&out_data);
			Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data);
			Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);

			out_tuple->delta.ts = in_tuple->delta.ts;
			out_tuple->delta.count = in_tuple->delta.count;
			std::memcpy(&out_tuple->data, &in_tuple->data, sizeof(Type));
		}
		in_data = nullptr;
		out_data = nullptr;
		this->in_node_left_->CleanCache();

		// process right input
		while (in_cache_right_->GetNext(&in_data)) {
			this->out_cache_->ReserveNext(&out_data);
			Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data);
			Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
			out_tuple->delta.ts = in_tuple->delta.ts;
			out_tuple->delta.count = (this->negate_left_) ? -in_tuple->delta.count : in_tuple->delta.count;
			std::memcpy(&out_tuple->data, &in_tuple->data, sizeof(Type));
		}

		this->in_node_right_->CleanCache();
	}

	void UpdateTimestamp(timestamp ts) {
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->ts_ = ts;
			this->in_node_left_->UpdateTimestamp(ts);
			this->in_node_right_->UpdateTimestamp(ts);
		}
	}

private:
	timestamp ts_;

	timestamp frontier_ts_;

	TypedNode<Type> *in_node_left_;
	TypedNode<Type> *in_node_right_;

	Cache *in_cache_left_;
	Cache *in_cache_right_;

	Cache *out_cache_;
	int out_count = 0;
	int clean_count = 0;

	bool negate_left_;
};

/**
 * @brief this will implement all but Compute functions for Stateful binary
 * nodes
 */
template <typename LeftType, typename RightType, typename OutType>
class StatefulBinaryNode : public TypedNode<OutType> {
public:
	StatefulBinaryNode(TypedNode<LeftType> *in_node_left, TypedNode<RightType> *in_node_right, Graph *graph,
	                   BufferPool *bp, index left_table_index, index right_table_index)
	    : in_node_left_ {in_node_left}, in_node_right_ {in_node_right}, in_cache_left_ {in_node_left->Output()},
	      in_cache_right_ {in_node_right->Output()}, frontier_ts_ {std::max(in_node_left->GetFrontierTs(),
	                                                                        in_node_right->GetFrontierTs())}, graph_{graph} {
		this->ts_ = get_current_timestamp();
		this->out_cache_ = new Cache(DEFAULT_CACHE_SIZE * 2, sizeof(Tuple<OutType>));

		// init table from graph metastate based on index

		// get reference to corresponding metastate
		MetaState &meta_left = this->graph_->tables_metadata_(left_table_index);

		this->left_table_ = new Table<LeftType>(meta_left.delta_filename_, meta_left.pages_, meta_left.btree_pages_, bp, graph_);

		// we also need to set ts for the node, we will use left ts for it, thus right ts will always be 0
		ts_ = meta_left.ts_;

		// get reference to corresponding metastate
		MetaState &meta_right = this->graph_->tables_metadata_(right_table_index);

		this->right_table_ = new Table<RightType>(meta_right.delta_filename_, meta_right.pages_, meta_right.btree_pages_, bp, graph_);
	}

	~StatefulBinaryNode() {
		delete this->out_cache_;
		delete this->left_table_;
		delete this->right_table_;
	}

	Cache *Output() {
		this->out_count++;
		return this->out_cache_;
	}
	void CleanCache() {
		clean_count++;
		if (this->clean_count == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count = 0;
		}
	}

	timestamp GetFrontierTs() const {
		return this->frontier_ts_;
	}
	virtual void Compute() = 0;

	void UpdateTimestamp(timestamp ts) {
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->compact_ = true;
			this->ts_ = ts;
			this->in_node_left_->UpdateTimestamp(ts);
			this->in_node_right_->UpdateTimestamp(ts);
		}
	}

protected:
	void Compact() {
		this->left_table_->MergeDeltas(this->ts_);
		this->right_table_->MergeDeltas(this->ts_);
	}

	// timestamp will be used to track valid tuples
	// after update propagate it to input nodes
	bool compact_ = false;
	timestamp &ts_;

	timestamp frontier_ts_;

	TypedNode<LeftType> *in_node_left_;
	TypedNode<RightType> *in_node_right_;

	Cache *in_cache_left_;
	Cache *in_cache_right_;

	Cache *out_cache_;
	int out_count = 0;
	int clean_count = 0;

	// we can treat whole tuple as a key, this will return it's index
	// we will later use some persistent storage for that mapping, maybe rocksdb
	// or something
	Table<LeftType> *left_table_;
	Table<RightType> *right_table_;

	Graph *graph_;
	
	std::mutex node_mutex;
};

// this is node behind Intersect
template <typename Type>
class IntersectNode : public StatefulBinaryNode<Type, Type, Type> {
public:
	IntersectNode(TypedNode<Type> *in_node_left, TypedNode<Type> *in_node_right, Graph *graph, BufferPool *bp,
	              index left_table_index, index right_table_index)
	    : StatefulBinaryNode<Type, Type, Type>(in_node_left, in_node_right, graph, bp, left_table_index,
	                                           right_table_index) {
	}

	void Compute() {
		// compute right_cache against left_cache
		// they are small and hold no indexes so we do it just by nested loop
		const char *in_data_left;
		const char *in_data_right;
		char *out_data;
		while (this->in_cache_left_->GetNext(&in_data_left)) {
			Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
			while (this->in_cache_right_->GetNext(&in_data_right)) {
				Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
				// if left and right caches match on data put it into out_cache with new
				// delta
				if (!std::memcmp(&in_left_tuple->data, &in_right_tuple->data, sizeof(Type))) {
					this->out_cache_->ReserveNext(&out_data);
					Delta out_delta = this->delta_function_(in_left_tuple->delta, in_right_tuple->delta);
					Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
					std::memcpy(&out_tuple->data, &in_left_tuple->data, sizeof(Type));
					out_tuple->delta = out_delta;
				}
			}
		}

		// compute left cache against right table
		while (this->in_cache_left_->GetNext(&in_data_left)) {
			Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);

			// get matching on data from left and iterate it's deltas
			index idx;
			if (this->right_table_->Search(in_left_tuple->data, &idx)) {
				std::multiset<Delta, DeltaComparator> &right_deltas = this->right_table_->Scan(idx);
				for (auto &right_delta : right_deltas) {
					this->out_cache_->ReserveNext(&out_data);
					Delta out_delta = this->delta_function_(in_left_tuple->delta, right_delta);
					Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
					std::memcpy(&out_tuple->data, &in_left_tuple->data, sizeof(Type));
					out_tuple->delta = out_delta;
				}
			}
		}

		// compute right cache against left table
		while (this->in_cache_right_->GetNext(&in_data_right)) {
			Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
			// get matching on data from right and iterate it's deltas
			index idx;
			if (this->left_table_->Search(in_right_tuple->data, &idx)) {
				std::multiset<Delta, DeltaComparator> &left_deltas = this->left_table_->Scan(idx);
				for (auto &left_delta : left_deltas) {
					this->out_cache_->ReserveNext(&out_data);
					Delta out_delta = this->delta_function_(left_delta, in_right_tuple->delta);
					Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
					std::memcpy(&out_tuple->data, &in_right_tuple->data, sizeof(Type));
					out_tuple->delta = out_delta;
				}
			}
		}

		// insert new deltas from in_caches
		while (this->in_cache_->GetNext(&in_data_right)) {
			Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
			index idx = this->right_table_->Insert(in_right_tuple->data);
			this->table_->InsertDelta(idx, in_right_tuple->delta);
		}
		while (this->in_cache_->GetNext(&in_data_left)) {
			Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
			index idx = this->left_table_->Insert(in_left_tuple->data);
			this->table_->InsertDelta(idx, in_left_tuple->delta);
		}

		// clean in_caches
		this->in_node_left_->CleanCache();
		this->in_node_right_->CleanCache();

		if (this->compact_) {
			this->Compact();
		}
	}

private:
	// extra state beyond StatefulNode is only deltafunction
	static Delta delta_function_(const Delta &left_delta, const Delta &right_delta) {
		return {std::max(left_delta.ts, right_delta.ts), left_delta.count * right_delta.count};
	}
};

template <typename InTypeLeft, typename InTypeRight, typename OutType>
class CrossJoinNode : public StatefulBinaryNode<InTypeLeft, InTypeRight, OutType> {
public:
	CrossJoinNode(TypedNode<InTypeLeft> *in_node_left, TypedNode<InTypeRight> *in_node_right,
	              std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout, Graph *graph,
	              BufferPool *bp, index left_table_index, index right_table_index)
	    : StatefulBinaryNode<InTypeLeft, InTypeRight, OutType>(in_node_left, in_node_right, graph, bp, left_table_index,
	                                                           right_table_index),
	      join_layout_ {join_layout} {
	}

	void Compute() {
		// compute right_cache against left_cache
		// they are small and hold no indexes so we do it just by nested loop
		const char *in_data_left;
		const char *in_data_right;
		char *out_data;
		while (this->in_cache_left_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			while (this->in_cache_right_->GetNext(&in_data_right)) {
				Tuple<InTypeRight> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
				// if left and right caches match on data put it into out_cache with new
				// delta
				this->out_cache_->ReserveNext(&out_data);
				Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
				out_tuple->delta = {std::max(in_left_tuple->delta.ts, in_right_tuple->delta.ts),
				                    in_left_tuple->delta.count * in_right_tuple->delta.count};
				out_tuple->data = this->join_layout_(in_left_tuple->data, in_right_tuple->data);
			}
		}

		// compute left cache against right table
		// right table
		index idx = 0;
		for (auto it = this->table_right_->HeapIterator.begin();
		     it != this->table_right_->HeapIterator.end(); ++it, idx++) {

			// left cache
			while (this->in_cache_left_->GetNext(&in_data_left)) {
				Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);

				// deltas from right table
				std::multiset<Delta, DeltaComparator> &right_deltas = this->right_table_->Scan(idx);
				for (auto &right_delta : right_deltas) {
					this->out_cache_->ReserveNext(&out_data);
					Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
					out_tuple->data = this->join_layout_(in_left_tuple->data, *it);
					out_tuple->delta = {std::max(in_left_tuple->delta.ts, right_delta.ts),
					                    in_left_tuple->delta.count * right_delta.count};
				}
			}
		}

		// compute right cache against left table
		// right table
		idx = 0;
		for (auto it = this->table_left_->HeapIterator.begin();
		     it != this->table_left_->HeapIterator.end(); ++it, idx++) {

			// left cache
			while (this->in_cache_right_->GetNext(&in_data_right)) {
				Tuple<InTypeLeft> *in_right_tuple = (Tuple<InTypeLeft> *)(in_data_right);

				// deltas from right table
				std::multiset<Delta, DeltaComparator> &left_deltas = this->left_table_->Scan(idx);
				for (auto &left_delta : left_deltas) {
					this->out_cache_->ReserveNext(&out_data);
					Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
					out_tuple->data = this->join_layout_(*it, in_right_tuple->data);
					out_tuple->delta = {std::max(in_right_tuple->delta.ts, left_delta.ts),
					                    in_right_tuple->delta.count * left_delta.count};
				}
			}
		}

		// insert new deltas from in_caches
		while (this->in_cache_->GetNext(&in_data_right)) {
			Tuple<InTypeRight> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
			index idx = this->table_->Insert(in_right_tuple->data);
			this->table_->InsertDelta(idx, in_right_tuple->delta);
		}
		while (this->in_cache_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			index idx = this->table_->Insert(in_left_tuple->data);
			this->table_->InsertDelta(idx, in_left_tuple->delta);
		}

		// clean in_caches
		this->in_node_left_->CleanCache();
		this->in_node_right_->CleanCache();

		if (this->compact_) {
			this->Compact();
		}
	}

private:
	// only state beyound StatefulNode state is join_layout_function
	std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout_;
};

// join on
// match type could be even char[] in this case
template <typename InTypeLeft, typename InTypeRight, typename MatchType, typename OutType>
class JoinNode : public StatefulBinaryNode<InTypeLeft, InTypeRight, OutType> {
public:
	JoinNode(TypedNode<InTypeLeft> *in_node_left, TypedNode<InTypeRight> *in_node_right,
	         std::function<MatchType(const InTypeLeft &)> get_match_left,
	         std::function<MatchType(const InTypeRight &)> get_match_right,
	         std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout, Graph *graph, BufferPool *bp,
	         index left_table_index, index right_table_index)
	    : StatefulBinaryNode<InTypeLeft, InTypeRight, OutType>(in_node_left, in_node_right, graph, bp, left_table_index,
	                                                           right_table_index),
	      get_match_left_(get_match_left), get_match_right_ {get_match_right}, join_layout_ {join_layout} {

		/** recompute matches */
		index idx = 0;
		for (auto it = this->left_table_->HeapIterator.begin(); it != this->left_table_->HeapIterator.end();
		     ++it, idx++) {

			MatchType match = this->get_match_left_(*it);
		
			if(!this->match_to_index_left_table_.contains(match)){
				this->match_to_index_left_table_[match] = {};
			}
			this->match_to_index_left_table_[match].insert(idx);
		}
		
		idx = 0;
		for (auto it = this->right_table_->HeapIterator.begin(); it != this->right_table_->HeapIterator.end();
		     ++it, idx++) {

			MatchType match = this->get_match_right_(*it);
		
			if(!this->match_to_index_right_table_.contains(match)){
				this->match_to_index_right_table_[match] = {};
			}
			this->match_to_index_right_table_[match].insert(idx);
		}

	}

	// this function changes
	void Compute() {
		// compute right_cache against left_cache
		// they are small and hold no indexes so we do it just by nested loop
		const char *in_data_left;
		const char *in_data_right;
		char *out_data;
		while (this->in_cache_left_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			while (this->in_cache_right_->GetNext(&in_data_right)) {
				Tuple<InTypeRight> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
				// if left and right caches match on data put it into out_cache with new
				// delta
				if (this->Compare(&in_left_tuple->data, &in_right_tuple->data)) {
					this->out_cache_->ReserveNext(&out_data);
					Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
					out_tuple->delta = {std::max(in_left_tuple->delta.ts, in_right_tuple->delta.ts),
					                    in_left_tuple->delta.count * in_right_tuple->delta.count};

					out_tuple->data = this->join_layout_(in_left_tuple->data, in_right_tuple->data);
				}
			}
		}

		// compute left cache against right table
		while (this->in_cache_left_->GetNext(&in_data_left)) {
			Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
			MatchType match = this->get_match_left_(in_left_tuple->data);
			for (const auto &idx : this->match_to_index_right_table_[Key<MatchType> match]) {
				InTypeRight right_data = this->right_table->Get(idx);
				// deltas from right table
				std::multiset<Delta, DeltaComparator> &right_deltas = this->right_table_->Scan(idx);
				// iterate all deltas of this tuple
				for (auto &right_delta : right_deltas) {
					this->out_cache_->ReserveNext(&out_data);
					Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
					out_tuple->data = this->join_layout_(in_left_tuple->data, &right_data);
					out_tuple->delta = {std::max(in_left_tuple->delta.ts, right_delta->ts),
					                    in_left_tuple->delta.count * right_delta->count};
				}
			}
		}

		// compute right cache against left table
		while (this->in_cache_right_->GetNext(&in_data_right)) {
			Tuple<InTypeLeft> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
			MatchType match = this->get_match_right_(in_right_tuple->data);
			for (const auto &idx : this->match_to_index_left_table_[Key<MatchType> match]) {
				InTypeLeft left_data = this->left_table->Get(idx);
				std::multiset<Delta, DeltaComparator> &left_deltas = this->left_table_->Scan(idx);
				// iterate all deltas of this tuple
				for (auto &left_delta : left_deltas) {
					this->out_cache_->ReserveNext(&out_data);
					Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
					out_tuple->data =  this->join_layout_(&left_data, in_right_tuple->data;
					out_tuple->delta = {std::max(in_right_tuple->delta.ts, left_delta->ts),
											in_right_tuple->delta.count * left_delta->count};
				}
			}
		}

		// insert new deltas from in_caches
		while (this->in_cache_->GetNext(&in_data_right)) {
			Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
			// and this will actually handle inserting into both match tree and normal btree
			index idx = this->table_->Insert(in_right_tuple->data);

			this->table_->InsertDelta(idx, in_right_tuple->delta);

			// track match on insert
			MatchType match = this->get_match_right_(in_data_right);
			if (!this->match_to_index_right_table_.contains(Key<MatchType> match)) {
				this->match_to_index_right_table_[Key<MatchType>(match)] = {}
			}
			this->match_to_index_right_table_[match].insert(idx);
		}

		while (this->in_cache_->GetNext(&in_data_left)) {
			Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
			// and this will actually handle inserting into both match tree and normal btree
			index idx = this->table_->Insert(in_left_tuple->data);
			this->table_->InsertDelta(idx, in_left_tuple->delta);

			// track match on insert
			MatchType match = this->get_match_left_(in_data_left);
			if (!this->match_to_index_left_table_.contains(Key<MatchType> match)) {
				this->match_to_index_left_table_[Key<MatchType>(match)] = {}
			}
			this->match_to_index_left_table_[match].insert(idx);
		}

		// clean in_caches
		this->in_node_left_->CleanCache();
		this->in_node_right_->CleanCache();

		if (this->compact_) {
			this->Compact();
		}
	}

private:
	inline bool Compare(InTypeLeft *left_data, InTypeRight *right_data) {
		MatchType left_match = this->get_match_left_(*left_data);
		MatchType right_match = this->get_match_right_(*right_data);
		return !std::memcmp(&left_match, &right_match, sizeof(MatchType));
	}

	// we need those functions to calculate matchfields from tuples on left and
	// righ
	std::function<MatchType(const InTypeLeft &)> get_match_left_;
	std::function<MatchType(const InTypeRight &)> get_match_right_;
	std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout_;

	Table<InTypeLeft> *left_table;
	Table<InTypeRight> *right_table;

	/**  we will store matches as non persistent storage,
	    on system start we will recalculate matches for all tuples,
	    then when inserting new tuples we will just compare their matches and check for indexes
	*/
	std::unordered_map<std::array<char, sizeof(MatchType)>, std::set<index>, KeyHash<MatchType>>
	    match_to_index_left_table_;
	std::unordered_map<std::array<char, sizeof(MatchType)>, std::set<index>, KeyHash<MatchType>>
	    match_to_index_right_table_;
};

/**
 * OK for this node we might want to use bit different model of computation
 * it will only emit single value at right time, and it need to delete old
 * value, it also need to compute next value based not only on data but also on
 * delta
 */

// and finally aggregate_by, so we are aggregating but also with grouping by
// fields what if we would store in type and out type, and then for all tuples
// would emit new version from their oldest version

// finally we will only aggregate by single field be it max sum etc,
// doesn't matter point is we aggregate on one thing only because for
// multiple aggregations we will be able to just chain them together

template <typename InType, typename MatchType>
class AggregateByNode : public TypedNode<OutType> {
	// now output of aggr might be also dependent of count of in tuple, for
	// example i sum inserting 5 Alice's should influence it different than one
	// Alice but with min it should not, so it will be dependent on aggregate
	// function
public:
	AggregateByNode(TypedNode<InType> *in_node, std::function<InType(const InType &, const InType &)> aggr_fun,
	                std::function<MatchType(const InType &)> get_match, Graph *graph, BufferPool *bp, index table_index)
	    : in_node_ {in_node}, in_cache_ {in_node->Output()},
	      frontier_ts_ {in_node->GetFrontierTs()}, aggr_fun_ {aggr_fun}, get_match_ {get_match}, graph_ {graph} {
		this->ts_ = get_current_timestamp();
		// there will be single tuple emited at once probably, if not it will get
		// resized so chill
		this->out_cache_ = new Cache(2, sizeof(Tuple<OutType>));

		// init table from graph metastate based on index

		// get reference to corresponding metastate
		MetaState &meta = this->graph_->tables_metadata_(table_index);

		this->table_ = new Table<InType>(meta.delta_filename_, meta.pages_, meta.btree_pages_, bp, graph_);

		// we also need to set ts for the node
		ts_ = meta.ts_;
	}

	~AggregateByNode() {
		delete out_cache_;
		delete table_;
	}

	Cache *Output() {
		this->out_count++;
		return this->out_cache_;
	}
	void CleanCache() {
		clean_count++;
		if (this->clean_count == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count = 0;
		}
	}

	timestamp GetFrontierTs() const {
		return this->frontier_ts_;
	}

	// output should be single tuple with updated values for different times
	// so what we can do there? if we emit new count as part of value, then it
	// will be treated as separate tuple if we emit new value as part of delta it
	// also will be wrong what if we would do : insert previous value with count
	// -1 and insert current with count 1 at the same time then old should get
	// discarded
	void Compute() {
		std::unordered_set<index> not_emited;

		// first insert all new data from cache to table
		const char *in_data_;
		while (this->in_cache_->GetNext(&in_data_)) {
			const Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data_);

			index idx = this->table_->Insert(in_tuple->data);
			bool was_present = this->table_->InsertDelta(idx, in_tuple->delta)

			                       if (!was_present) {
				not_emited.insert(idx);
			}
		}

		// and then we emit insert and delete based on index at the same time

		// itreate all tuples, if it's in match of inserted:
		// compute aggregate for it

		// ok how can we know that some tuple was already emited?
		// if our oldest delta timestamp is less than or equal to previous ts,
		// it means than it was already emited as insert, so we can emit this value with delete
		// otherwise, there was no previous version and we can safely delete

		// now onto the way in which we will iterate: we need to iterate all tuples
		// so in this node our table will be normal one and our match will be temoprar and not persistent

		std::unordered_map<MatchType, InType> matches_;
		for (int index = 0, auto it = this->table_->HeapIterator.begin(); it != this->table_->HeapIterator.end();
		     ++it, index++) {

			Delta &olders_delta = *this->table_->Scan(index).rbegin();
			// check if it oldest if previous ts that will mean it was already emited
			if (olders_delta.ts <= this->previous_ts_) {
				if (matches_from_queue.contains(get_match_(*it))) {
					matches_[get_match_(*it)] = *it;
				} else {
					matches_[get_match_(*it)] = aggr_fun_(matches_[get_match_(*it)], *it);
				}
			}
		}
		// emit all matches from queue with delete
		for (const auto &[_, in_data] : matches_) {
			char *out_data;
			this->out_cache_->ReserveNext(&out_data);
			Tuple<InType> *del_tuple = (Tuple<InType> *)(out_data);
			del_tuple->delta = {this->previous_ts_, -1};
			std::memcpy(&del_tuple->data, in_data, sizeof(OutType));
		}

		this->Compact();

		this->compact_ = false;

		std::unordered_map<MatchType, InType> matches_ = {};
		for (int index = 0, auto it = this->table_->HeapIterator.begin(); it != this->table_->HeapIterator.end();
		     ++it, index++) {

			Delta &olders_delta = *this->table_->Scan(index).rbegin();
			// check if it oldest if previous ts that will mean it was already emited
			if (olders_delta.ts <= this->previous_ts_) {
				if (matches_from_queue.contains(get_match_(*it))) {
					matches_[get_match_(*it)] = *it;
				} else {
					matches_[get_match_(*it)] = aggr_fun_(matches_[get_match_(*it)], *it);
				}
			}
		}
		// emit all matches from queue with delete
		for (const auto &[_, in_data] : matches_) {
			char *out_data;
			this->out_cache_->ReserveNext(&out_data);
			Tuple<InType> *ins_tuple = (Tuple<InType> *)(out_data);
			ins_tuple->delta = {this->previous_ts_, 11};
			std::memcpy(&ins_tuple->data, in_data, sizeof(OutType));
		}
	}

	void UpdateTimestamp(timestamp ts) {
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->previous_ts_ = this->ts_;
			this->ts_ = ts;
			this->compact_ = true;
			this->in_node_->UpdateTimestamp(ts);
		}
	}

private:
	// let's store full version data and not deltas in this node, so we can just
	// discard old versions
	void Compact() {
		// compact all the way to previous version, we need it to to emit delete later
		this->table_->MergeDelta(this->previous_ts_);
	}

	// timestamp will be used to track valid tuples
	// after update propagate it to input nodes

	size_t next_index_ = 0;

	bool compact_ = false;
	timestamp &ts_;
	timestamp previous_ts_;

	timestamp frontier_ts_;

	TypedNode<InType> *in_node_;

	Cache *in_cache_;

	Cache *out_cache_;
	int out_count = 0;
	int clean_count = 0;

	std::function<InType(const InType &, const InType &)> aggr_fun_;
	std::function<MatchType(const InType &)> get_match_;

	Table<Type> *table_;

	std::mutex node_mutex;

	Graph *graph_;
};

} // namespace AliceDB
#endif
