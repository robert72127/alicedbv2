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

namespace AliceDB {

struct MetaState {
	std::vector<index> pages_;
	std::vector<index> btree_pages_;
	std::set<index> recompute_idexes_;
	std::set<index> not_emited_;
	std::string delta_filename_;
	timestamp previous_ts_;
	index table_idx_;
};

// we need this definition to store graph pointer in node
class Graph;
class BufferPool;

class Node {
public:
	Node(timestamp frontier_ts) : frontier_ts_ {frontier_ts} {
	}

	virtual ~Node() {};

	virtual bool Compute() = 0;

	virtual void CleanCache() = 0;
	/**
	 * @brief update lowest ts this node will need to hold
	 */
	virtual void UpdateTimestamp() = 0;

	void AddOutNode(Node *node) {
		out_nodes_.push_back(node);
	}

	/** @brief return oldest ts that we need to keep */
	timestamp OldestTsToKeep() {
		timestamp ts = get_current_timestamp();
		for (const auto out : out_nodes_) {
			ts = std::min(ts, out->GetTs());
		}
		return ts;
	}

	timestamp GetTs() const {
		return ts_;
	}

	timestamp GetFrontierTs() const {
		return this->frontier_ts_;
	}

protected:
	// list of out nodes for keeping track of oldest time we need to store
	std::vector<Node *> out_nodes_;

	// what is oldest timestamp that needs to be keept by this table
	timestamp ts_;

	// how much time back from current time do we have to store values
	timestamp frontier_ts_;
};

template <typename OutType>
class TypedNode : public Node {
public:
	TypedNode(timestamp frontier_ts) : Node {frontier_ts} {
	}

	using value_type = OutType;

	virtual ~TypedNode() {};

	/**
	 * @brief returns cache corresponding to output from this Tuple
	 */
	virtual Cache<OutType> *Output() = 0;
};

// add new producers here
enum class ProducerType { FILE, TCPCLIENT };

/**  @brief Source node is responsible for producing data through Compute function and
 * then writing output to both out_cache, and persistent table creator of this
 * node needs to specify how long delayed data might arrive
 */
template <typename Type>
class SourceNode : public TypedNode<Type> {
public:
	SourceNode(ProducerType prod_type, const std::string &producer_source,
	           std::function<bool(std::istringstream &, Type *)> parse_input, timestamp frontier_ts, int duration_us,
	           Graph *graph)
	    : graph_ {graph}, TypedNode<Type> {frontier_ts}, duration_us_ {duration_us} {

		// init producer from args
		switch (prod_type) {
		case ProducerType::FILE:
			this->produce_ = std::make_unique<FileProducer<Type>>(producer_source, parse_input);
			break;
		case ProducerType::TCPCLIENT:
			this->produce_ = std::make_unique<TCPClientProducer<Type>>(producer_source, parse_input);
			break;
		}

		this->produce_cache_ = new Cache<Type>(DEFAULT_CACHE_SIZE);
		this->ts_ = get_current_timestamp();
	}

	~SourceNode() {
		delete produce_cache_;
	}

	bool Compute() {
		bool produced = false;

		auto start = std::chrono::steady_clock::now();
		std::chrono::microseconds duration(this->duration_us_);
		auto end = start + duration;
		// produce some data with time limit set, into produce_cache
		while (std::chrono::steady_clock::now() < end) {
			Tuple<Type> prod_tuple;
			bool success = this->produce_->next(&prod_tuple);
			if (!success) {
				break;
			}
			produced = true;
			produce_cache_->Insert(prod_tuple);
		}

		this->produce_cache_->FinishInserting();
		return produced;
	}

	Cache<Type> *Output() {
		this->out_count++;
		return this->produce_cache_;
	}

	void CleanCache() {
		this->clean_count.fetch_add(1);
		if (this->clean_count.load() == this->out_count) {
			this->produce_cache_->Clean();
			this->clean_count.exchange(0);
		}
	}

	void UpdateTimestamp() {
		timestamp ts = this->OldestTsToKeep();
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->ts_ = ts;
		}
		return;
	}

private:
	std::unique_ptr<Producer<Type>> produce_;

	Graph *graph_;

	// data from producer is put into this cache from it it's written into both
	// table and passed to output nodes
	Cache<Type> *produce_cache_;
	int out_count = 0;
	std::atomic<int> clean_count {0};

	int duration_us_;
};

/**
 * @brief out node that collect outputs and allow for iterating internal storage
 */
template <typename Type>
class SinkNode : public TypedNode<Type> {
public:
	SinkNode(TypedNode<Type> *in_node, Graph *graph, BufferPool *bp, GarbageCollectSettings &gb_settings,
	         MetaState &meta, index table_index)
	    : graph_ {graph}, in_node_(in_node), in_cache_ {in_node->Output()},
	      gb_settings_ {gb_settings}, meta_ {meta}, TypedNode<Type> {in_node->GetFrontierTs()} {

		this->ts_ = get_current_timestamp();
		this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;

		// init table from graph metastate based on index
		this->table_ = new Table<Type>(meta.delta_filename_, meta.pages_, meta.btree_pages_, bp, graph_);
	}

	~SinkNode() {
		delete table_;
	}

	bool Compute() {
		// write in cache into out_table
		const char *in_data;
		while (this->in_cache_->HasNext()) {
			Tuple<Type> in_tuple = this->in_cache_->GetNext();
			index idx = this->table_->Insert(in_tuple.data);
			this->table_->InsertDelta(idx, in_tuple.delta);
		}

		// get current timestamp that can be considered
		this->UpdateTimestamp();

		if (this->compact_) {
			this->Compact();
		}
		this->in_node_->CleanCache();

		// periodically call garbage collector
		if (this->gb_settings_.use_garbage_collector && this->next_clean_ts_ < this->ts_) {
			this->table_->GarbageCollect(this->ts_ - this->gb_settings_.delete_age_,
			                             this->gb_settings_.remove_zeros_only);
			this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;
		}

		// never produces
		return false;
	}

	// since all sink does is store state we can treat incache as out cache when we use sink(view)
	// as source
	Cache<Type> *Output() {
		return this->in_node_->Output();
	}
	void CleanCache() {
		this->in_node_->CleanCache();
	}

	void UpdateTimestamp() {
		timestamp ts = get_current_timestamp();
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->compact_ = true;
			this->ts_ = ts;
			this->in_node_->UpdateTimestamp();
		}
	}

	class Iterator {
	public:
		Iterator(HeapIterator<Type> iter, Table<Type> *table, timestamp ts)
		    : heap_iterator_(iter), table_ {table}, ts_ {ts} {
		}

		Iterator &operator++() {
			++this->heap_iterator_;
			return *this;
		}

		Tuple<Type> operator*() {

			auto [data, idx] = heap_iterator_.Get();
			Tuple<Type> tpl;
			tpl.data = *data;
			tpl.delta.count = 0;
			for (auto dit : this->table_->Scan(idx)) {
				if (dit.ts > this->ts_) {
					break;
				} else {
					tpl.delta.count += dit.count;
					tpl.delta.ts = dit.ts;
				}
			}
			return tpl;
		}

		bool operator!=(const Iterator &other) const {
			return this->heap_iterator_ != other.heap_iterator_;
		}

	private:
		HeapIterator<Type> heap_iterator_;
		Table<Type> *table_;
		timestamp ts_;
	};

	Iterator begin(timestamp ts) {
		return Iterator(this->table_->begin(), this->table_, ts);
	}

	Iterator end() {
		return Iterator(this->table_->end(), this->table_, 0);
	}

private:
	void Compact() {
		this->table_->MergeDelta(this->ts_);
	}

	Graph *graph_;

	TypedNode<Type> *in_node_;

	Table<Type> *table_;

	// in cache only, since sink isn't processing getting output from this node will also return output from this cache
	Cache<Type> *in_cache_;

	GarbageCollectSettings &gb_settings_;
	timestamp next_clean_ts_;

	MetaState &meta_;

	bool compact_;
};

/** @brief stateless node that filters tuples based on condition function */
template <typename Type>
class FilterNode : public TypedNode<Type> {
public:
	FilterNode(TypedNode<Type> *in_node, std::function<bool(const Type &)> condition, Graph *graph)
	    : graph_ {graph}, condition_ {condition}, in_node_ {in_node}, in_cache_ {in_node->Output()},
	      TypedNode<Type> {in_node->GetFrontierTs()} {
		this->out_cache_ = new Cache<Type>(DEFAULT_CACHE_SIZE);
		this->ts_ = get_current_timestamp();
	}

	~FilterNode() {
		delete out_cache_;
	}

	// filters node
	// those that pass are put into output cache, this is all that this node does
	bool Compute() {
		bool produced = false;
		// pass function that match condition to output
		while (this->in_cache_->HasNext()) {
			Tuple<Type> tuple = this->in_cache_->GetNext();
			if (this->condition_(tuple.data)) {
				this->out_cache_->Insert(tuple);
				produced = true;
			}
		}
		this->out_cache_->FinishInserting();
		this->in_node_->CleanCache();

		return produced;
	}

	Cache<Type> *Output() {
		this->out_count++;
		return this->out_cache_;
	}

	void CleanCache() {
		this->clean_count.fetch_add(1);
		if (this->clean_count.load() == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count.exchange(0);
		}
	}

	// timestamp is not used here but will be used for global state for
	// propagation
	void UpdateTimestamp() {
		timestamp ts = this->OldestTsToKeep();
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->ts_ = ts;
			this->in_node_->UpdateTimestamp();
		}
	}

private:
	Graph *graph_;

	TypedNode<Type> *in_node_;

	std::function<bool(const Type &)> condition_;

	Cache<Type> *in_cache_;
	Cache<Type> *out_cache_;
	int out_count = 0;
	std::atomic<int> clean_count {0};
};
// projection can be represented by single node
template <typename InType, typename OutType>
class ProjectionNode : public TypedNode<OutType> {
public:
	ProjectionNode(TypedNode<InType> *in_node, std::function<OutType(const InType &)> projection, Graph *graph)
	    : projection_ {projection}, graph_ {graph}, in_node_ {in_node}, in_cache_ {in_node->Output()},
	      TypedNode<OutType> {in_node->GetFrontierTs()} {
		this->out_cache_ = new Cache<OutType>(DEFAULT_CACHE_SIZE);
		this->ts_ = get_current_timestamp();
	}

	~ProjectionNode() {
		delete out_cache_;
	}

	bool Compute() {
		bool produced = false;
		while (in_cache_->HasNext()) {
			Tuple<InType> in_tuple = this->in_cache_->GetNext();
			OutType out_tuple = this->projection_(in_tuple.data);
			out_cache_->Insert(out_tuple, in_tuple.delta);
			produced = true;
		}

		this->out_cache_->FinishInserting();
		this->in_node_->CleanCache();

		return produced;
	}

	Cache<OutType> *Output() {
		this->out_count++;
		return this->out_cache_;
	}

	void CleanCache() {
		this->clean_count.fetch_add(1);
		if (this->clean_count.load() == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count.exchange(0);
		}
	}

	// timestamp is not used here but might be helpful to store for propagatio
	void UpdateTimestamp() {
		timestamp ts = this->OldestTsToKeep();
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->ts_ = ts;
			this->in_node_->UpdateTimestamp();
		}
	}

private:
	std::function<OutType(const InType &)> projection_;

	Graph *graph_;

	// track this for global timestamp state update
	TypedNode<InType> *in_node_;

	Cache<InType> *in_cache_;
	Cache<OutType> *out_cache_;
	int out_count = 0;
	std::atomic<int> clean_count {0};
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
	DistinctNode(TypedNode<Type> *in_node, Graph *graph, BufferPool *bp, GarbageCollectSettings &gb_settings,
	             MetaState &meta, index table_index)
	    : graph_ {graph}, in_cache_ {in_node->Output()}, in_node_ {in_node},
	      table_index_(table_index), gb_settings_ {gb_settings}, meta_ {meta},
	      TypedNode<Type> {in_node->GetFrontierTs()}, previous_ts_ {meta.previous_ts_},
	      recompute_indexes_ {meta.recompute_idexes_}, not_emited_ {meta.not_emited_} {

		this->ts_ = get_current_timestamp();
		this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;
		this->out_cache_ = new Cache<Type>(DEFAULT_CACHE_SIZE);

		// init table from graph metastate based on index

		this->table_ = new Table<Type>(meta.delta_filename_, meta.pages_, meta.btree_pages_, bp, graph_);
	}

	~DistinctNode() {
		// update meta ts

		delete out_cache_;
		delete table_;
	}

	Cache<Type> *Output() {
		this->out_count++;
		return this->out_cache_;
	}

	void CleanCache() {
		this->clean_count.fetch_add(1);
		if (this->clean_count.load() == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count.exchange(0);
		}
	}

	bool Compute() {
		bool produced = false;

		// first insert all new data from cache to table
		while (this->in_cache_->HasNext()) {
			const Tuple<Type> in_tuple = this->in_cache_->GetNext();
			index idx = this->table_->Insert(in_tuple.data);
			bool was_present = this->table_->InsertDelta(idx, in_tuple.delta);

			recompute_indexes_.insert(idx);
			if (!was_present) {
				this->not_emited_.insert(idx);
			}
		}

		/*
		           We can deduce what to emit based on this index value from
		           oldest_delta, negative delta -> previouse state was 0 positive delta
		           -> previouse state was 1

		                if previous state was 0
		                        if now positive emit 1
		                        if now negative don't emit
		                if previouse state was 1
		                        if now positive don't emit
		                        if now negative emit -1

		                if it's first iteration of this Node we need to always emit if positive


		    so if it's time to emit:

		    we iter all indexes, and then:


		    if oldest version is newer than previous ts we need to always emit
		    else we either emit or not based on: oldest delta, and deltas up to current timestamp

		    then we can compact
		*/
		if (this->compact_) {
			// delta_count for oldest keept verson fro this we can deduce what tuples
			// to emit;
			//  out node will always have count of each tuple as either 0 or 1

			// iterate only through tuples that needs recomputation
			for (const auto &idx : recompute_indexes_) {
				// iterate by delta tuple, ok since tuples are appeneded sequentially we can get index from tuple
				// position using heap iterator, this should be fast since distinct shouldn't store that many tuples

				std::vector<Delta> current_idx_deltas = this->table_->Scan(idx);

				// whether previous emited tuple delta had positive count
				bool previous_positive = current_idx_deltas[0].count > 0;
				// this means this tuple was never emited, then we discard previous positive, since it's incorrect
				/** @todo this might fail if oldest tuple was inserted with lower timestamp than ts_  same for logic in
				 * aggregate */
				bool prev_not_emited = this->not_emited_.contains(idx);

				int count = 0;
				for (const auto &delta : current_idx_deltas) {
					if (delta.ts > this->ts_) {
						break;
					}
					count += delta.count;
				}
				bool current_positive = count > 0;

				if ((prev_not_emited && current_positive) ||
				    (previous_positive && !current_positive && !prev_not_emited) ||
				    (!previous_positive && current_positive && !prev_not_emited)) {
					Type tp = this->table_->Get(idx);
					this->out_cache_->Insert(tp, Delta {this->ts_, current_positive ? 1 : -1});
					produced = true;
				}
			}

			this->Compact(); // after this iteration oldest version keep will be one with timestamp of current compact
			this->compact_ = false;
			recompute_indexes_ = {};
			not_emited_ = {};
			this->out_cache_->FinishInserting();
		}

		// periodically call garbage collector
		if (this->gb_settings_.use_garbage_collector && this->next_clean_ts_ < this->ts_) {
			this->table_->GarbageCollect(this->ts_ - this->gb_settings_.delete_age_,
			                             this->gb_settings_.remove_zeros_only);
			this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;
		}

		this->in_node_->CleanCache();

		return produced;
	}

	void UpdateTimestamp() {
		timestamp ts = this->OldestTsToKeep();
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->compact_ = true;
			this->previous_ts_ = this->ts_;
			this->ts_ = ts;
			this->in_node_->UpdateTimestamp();
		}
	}

private:
	void Compact() {
		// leave previous_ts version as oldest one
		this->table_->MergeDelta(this->previous_ts_);
	}

	Graph *graph_;

	TypedNode<Type> *in_node_;

	Table<Type> *table_;
	index table_index_;

	Cache<Type> *in_cache_;
	Cache<Type> *out_cache_;
	int out_count = 0;
	std::atomic<int> clean_count {0};

	GarbageCollectSettings &gb_settings_;
	timestamp next_clean_ts_;

	MetaState &meta_;

	// timestamp will be used to track valid tuples
	// after update propagate it to input nodes
	bool compact_ = false;

	timestamp &previous_ts_;

	std::set<index> &recompute_indexes_;
	// list of indexes that were not emited yet
	std::set<index> &not_emited_;
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
	PlusNode(TypedNode<Type> *in_node_left, TypedNode<Type> *in_node_right, bool negate_left, Graph *graph)
	    : graph_ {graph}, in_node_left_ {in_node_left}, in_node_right_ {in_node_right},
	      in_cache_left_ {in_node_left->Output()}, in_cache_right_ {in_node_right->Output()},
	      TypedNode<Type> {std::max(in_node_left->GetFrontierTs(), in_node_right->GetFrontierTs())},
	      negate_left_(negate_left) {
		this->ts_ = get_current_timestamp();
		this->out_cache_ = new Cache<Type>(DEFAULT_CACHE_SIZE);
	}

	~PlusNode() {
		delete out_cache_;
	}

	Cache<Type> *Output() {
		this->out_count++;
		return this->out_cache_;
	}

	void CleanCache() {
		this->clean_count.fetch_add(1);
		if (this->clean_count.load() == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count.exchange(0);
		}
	}

	bool Compute() {
		bool produced = false;

		// process left input
		const char *in_data;
		char *out_data;
		while (in_cache_left_->HasNext()) {
			Tuple<Type> in_tuple = in_cache_left_->GetNext();
			out_cache_->Insert(in_tuple);
			produced = true;
		}
		this->in_node_left_->CleanCache();

		// process right input
		while (in_cache_right_->HasNext()) {
			Tuple<Type> in_tuple = in_cache_right_->GetNext();
			out_cache_->Insert(in_tuple);
			produced = true;
		}
		this->in_node_right_->CleanCache();

		this->out_cache_->FinishInserting();

		return produced;
	}

	void UpdateTimestamp() {
		timestamp ts = this->OldestTsToKeep();
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->ts_ = ts;
			this->in_node_left_->UpdateTimestamp();
			this->in_node_right_->UpdateTimestamp();
		}
	}

private:
	Graph *graph_;

	TypedNode<Type> *in_node_left_;
	TypedNode<Type> *in_node_right_;

	Cache<Type> *in_cache_left_;
	Cache<Type> *in_cache_right_;

	Cache<Type> *out_cache_;
	int out_count = 0;
	std::atomic<int> clean_count {0};

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
	                   BufferPool *bp, GarbageCollectSettings &gb_settings, MetaState &left_meta, MetaState &right_meta,
	                   index left_table_index, index right_table_index)
	    : graph_ {graph}, in_node_left_ {in_node_left}, in_node_right_ {in_node_right},
	      in_cache_left_ {in_node_left->Output()}, in_cache_right_ {in_node_right->Output()},
	      gb_settings_ {gb_settings}, left_meta_ {left_meta}, right_meta_ {right_meta},
	      TypedNode<OutType> {std::max(in_node_left->GetFrontierTs(), in_node_right->GetFrontierTs())} {

		this->ts_ = get_current_timestamp();
		this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;

		this->out_cache_ = new Cache<OutType>(DEFAULT_CACHE_SIZE * 2);

		// init table from graph metastate based on index

		this->left_table_ =
		    new Table<LeftType>(left_meta.delta_filename_, left_meta.pages_, left_meta.btree_pages_, bp, graph_);

		// we also need to set ts for the node, we will use left ts for it, thus right ts will always be 0

		// get reference to corresponding metastate

		this->right_table_ =
		    new Table<RightType>(left_meta.delta_filename_, left_meta.pages_, left_meta.btree_pages_, bp, graph_);
	}

	~StatefulBinaryNode() {
		delete this->out_cache_;
		delete this->left_table_;
		delete this->right_table_;
	}

	Cache<OutType> *Output() {
		this->out_count++;
		return this->out_cache_;
	}

	void CleanCache() {
		this->clean_count.fetch_add(1);
		if (this->clean_count.load() == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count.exchange(0);
		}
	}

	virtual bool Compute() = 0;

	void UpdateTimestamp() {
		timestamp ts = this->OldestTsToKeep();
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->compact_ = true;
			this->ts_ = ts;
			this->in_node_left_->UpdateTimestamp();
			this->in_node_right_->UpdateTimestamp();
		}
	}

protected:
	void Compact() {
		this->left_table_->MergeDelta(this->ts_);
		this->right_table_->MergeDelta(this->ts_);
	}

	Graph *graph_;

	TypedNode<LeftType> *in_node_left_;
	TypedNode<RightType> *in_node_right_;

	Table<LeftType> *left_table_;
	Table<RightType> *right_table_;

	Cache<LeftType> *in_cache_left_;
	Cache<RightType> *in_cache_right_;

	Cache<OutType> *out_cache_;
	int out_count = 0;
	std::atomic<int> clean_count {0};

	GarbageCollectSettings &gb_settings_;
	timestamp next_clean_ts_;

	MetaState &left_meta_;
	MetaState &right_meta_;

	// timestamp will be used to track valid tuples
	// after update propagate it to input nodes
	bool compact_ = false;
};

// this is node behind Intersect
template <typename Type>
class IntersectNode : public StatefulBinaryNode<Type, Type, Type> {
public:
	IntersectNode(TypedNode<Type> *in_node_left, TypedNode<Type> *in_node_right, Graph *graph, BufferPool *bp,
	              GarbageCollectSettings &gb_settings, MetaState &left_meta, MetaState &right_meta,
	              index left_table_index, index right_table_index)
	    : StatefulBinaryNode<Type, Type, Type>(in_node_left, in_node_right, graph, bp, gb_settings, left_meta,
	                                           right_meta, left_table_index, right_table_index) {
	}

	bool Compute() {
		bool produced = false;

		// compute right_cache against left_cache
		// they are small and hold no indexes so we do it just by nested loop
		while (this->in_cache_left_->HasNext()) {
			Tuple<Type> in_left_tuple = this->in_cache_left_->GetNext();
			while (this->in_cache_right_->HasNext()) {
				Tuple<Type> in_right_tuple = this->in_cache_right_->GetNext();
				// if left and right caches match on data put it into out_cache with new delta
				if (std::memcmp(&in_left_tuple.data, &in_right_tuple.data, sizeof(Type)) == 0) {
					this->out_cache_->Insert(in_left_tuple.data,
					                         this->delta_function_(in_left_tuple.delta, in_right_tuple.delta));
					produced = true;
				}
			}
		}

		// compute left cache against right table
		while (this->in_cache_left_->HasNext()) {
			Tuple<Type> in_left_tuple = this->in_cache_left_->GetNext();

			// get matching on data from left and iterate it's deltas
			index idx;
			if (this->right_table_->Search(in_left_tuple.data, &idx)) {
				const std::vector<Delta> &right_deltas = this->right_table_->Scan(idx);

				Tuple<Type> out_tuple;
				out_tuple.data = in_left_tuple.data;
				for (auto &right_delta : right_deltas) {
					Delta out_delta = this->delta_function_(in_left_tuple.delta, right_delta);
					out_tuple.delta = out_delta;
					this->out_cache_->Insert(out_tuple);
					produced = true;
				}
			}
		}

		// compute right cache against left table
		while (this->in_cache_right_->HasNext()) {

			Tuple<Type> in_right_tuple = this->in_cache_right_->GetNext();

			// get matching on data from right and iterate it's deltas
			index idx;
			if (this->left_table_->Search(in_right_tuple.data, &idx)) {
				const std::vector<Delta> &left_deltas = this->left_table_->Scan(idx);

				Tuple<Type> out_tuple;
				out_tuple.data = in_right_tuple.data;
				for (auto &left_delta : left_deltas) {
					out_tuple.delta = this->delta_function_(left_delta, in_right_tuple.delta);
					this->out_cache_->Insert(out_tuple);
					produced = true;
				}
			}
		}

		this->out_cache_->FinishInserting();

		// insert new deltas from in_caches
		while (this->in_cache_right_->HasNext()) {
			Tuple<Type> in_right_tuple = this->in_cache_right_->GetNext();
			index idx = this->right_table_->Insert(in_right_tuple.data);
			this->right_table_->InsertDelta(idx, in_right_tuple.delta);
		}
		while (this->in_cache_left_->HasNext()) {
			Tuple<Type> in_left_tuple = this->in_cache_left_->GetNext();
			index idx = this->left_table_->Insert(in_left_tuple.data);
			this->left_table_->InsertDelta(idx, in_left_tuple.delta);
		}

		// clean in_caches
		this->in_node_left_->CleanCache();
		this->in_node_right_->CleanCache();

		if (this->compact_) {
			this->Compact();
		}

		// periodically call garbage collector
		if (this->gb_settings_.use_garbage_collector && this->next_clean_ts_ < this->ts_) {
			this->left_table_->GarbageCollect(this->ts_ - this->gb_settings_.delete_age_,
			                                  this->gb_settings_.remove_zeros_only);
			this->right_table_->GarbageCollect(this->ts_ - this->gb_settings_.delete_age_,
			                                   this->gb_settings_.remove_zeros_only);
			this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;
		}

		return produced;
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
	              BufferPool *bp, GarbageCollectSettings &gb_settings, MetaState &left_meta, MetaState &right_meta,
	              index left_table_index, index right_table_index)
	    : StatefulBinaryNode<InTypeLeft, InTypeRight, OutType>(in_node_left, in_node_right, graph, bp, gb_settings,
	                                                           left_meta, right_meta, left_table_index,
	                                                           right_table_index),
	      join_layout_ {join_layout} {
	}

	bool Compute() {
		bool produced = false;

		// compute right_cache against left_cache
		// they are small and hold no indexes so we do it just by nested loop
		while (this->in_cache_left_->HasNext()) {
			Tuple<InTypeLeft> in_left_tuple = this->in_cache_left_->GetNext();
			while (this->in_cache_right_->HasNext()) {
				Tuple<InTypeRight> in_right_tuple = this->in_cache_right_->GetNext();
				// if left and right caches match on data put it into out_cache with new delta

				Tuple<OutType> out_tuple;
				out_tuple.delta = {std::max(in_left_tuple.delta.ts, in_right_tuple.delta.ts),
				                   in_left_tuple.delta.count * in_right_tuple.delta.count};

				out_tuple.data = this->join_layout_(in_left_tuple.data, in_right_tuple.data);
				this->out_cache_->Insert(out_tuple);
				produced = true;
			}
		}

		// compute left cache against right table
		// right table
		for (auto it = this->right_table_->begin(); it != this->right_table_->end(); ++it) {
			auto [data, idx] = it.Get();
			// left cache
			while (this->in_cache_left_->HasNext()) {
				Tuple<InTypeLeft> in_left_tuple = this->in_cache_left_->GetNext();

				// deltas from right table
				const std::vector<Delta> &right_deltas = this->right_table_->Scan(idx);
				Tuple<OutType> out_tuple;
				out_tuple.data = this->join_layout_(in_left_tuple.data, *data);
				for (auto &right_delta : right_deltas) {
					out_tuple.delta = {std::max(in_left_tuple.delta.ts, right_delta.ts),
					                   in_left_tuple.delta.count * right_delta.count};
					this->out_cache_->Insert(out_tuple);
					produced = true;
				}
			}
		}

		// compute right cache against left table
		// right table
		for (auto it = this->left_table_->begin(); it != this->left_table_->end(); ++it) {
			auto [data, idx] = it.Get();
			// left cache
			while (this->in_cache_right_->HasNext()) {
				Tuple<InTypeLeft> in_right_tuple = this->in_cache_right_->GetNext();

				// deltas from right table
				const std::vector<Delta> &left_deltas = this->left_table_->Scan(idx);
				Tuple<OutType> out_tuple;
				out_tuple.data = this->join_layout_(*data, in_right_tuple.data);
				for (auto &left_delta : left_deltas) {
					out_tuple.delta = {std::max(in_right_tuple.delta.ts, left_delta.ts),
					                   in_right_tuple.delta.count * left_delta.count};
					this->out_cache_->Insert(out_tuple);
					produced = true;
				}
			}
		}

		this->out_cache_->FinishInserting();

		// insert new deltas from in_caches
		while (this->in_cache_right_->HasNext()) {
			Tuple<InTypeRight> in_right_tuple = this->in_cache_right_->GetNext();
			index idx = this->right_table_->Insert(in_right_tuple.data);
			this->right_table_->InsertDelta(idx, in_right_tuple.delta);
		}
		while (this->in_cache_left_->HasNext()) {
			Tuple<InTypeLeft> in_left_tuple = this->in_cache_left_->GetNext();
			index idx = this->left_table_->Insert(in_left_tuple.data);
			this->left_table_->InsertDelta(idx, in_left_tuple.delta);
		}

		// clean in_caches
		this->in_node_left_->CleanCache();
		this->in_node_right_->CleanCache();

		if (this->compact_) {
			this->Compact();
		}

		// periodically call garbage collector
		if (this->gb_settings_.use_garbage_collector && this->next_clean_ts_ < this->ts_) {
			this->left_table_->GarbageCollect(this->ts_ - this->gb_settings_.delete_age_,
			                                  this->gb_settings_.remove_zeros_only);
			this->right_table_->GarbageCollect(this->ts_ - this->gb_settings_.delete_age_,
			                                   this->gb_settings_.remove_zeros_only);
			this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;
		}

		return produced;
	}

private:
	// only state beyound StatefulNode state is join_layout_function
	std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout_;
};

// join on match type
template <typename InTypeLeft, typename InTypeRight, typename MatchType, typename OutType>
class JoinNode : public TypedNode<OutType> {
public:
	JoinNode(TypedNode<InTypeLeft> *in_node_left, TypedNode<InTypeRight> *in_node_right,
	         std::function<MatchType(const InTypeLeft &)> get_match_left,
	         std::function<MatchType(const InTypeRight &)> get_match_right,
	         std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout, Graph *graph, BufferPool *bp,
	         GarbageCollectSettings &gb_settings, MetaState &left_meta, MetaState &right_meta, index left_table_index,
	         index right_table_index)
	    : graph_ {graph}, in_node_left_ {in_node_left}, in_node_right_ {in_node_right},
	      in_cache_left_ {in_node_left->Output()}, in_cache_right_ {in_node_right->Output()},
	      gb_settings_ {gb_settings}, left_meta_ {left_meta}, right_meta_ {right_meta},
	      TypedNode<OutType> {std::max(in_node_left->GetFrontierTs(), in_node_right->GetFrontierTs())},
	      get_match_left_(get_match_left), get_match_right_ {get_match_right}, join_layout_ {join_layout} {

		this->ts_ = get_current_timestamp();
		this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;

		this->out_cache_ = new Cache<OutType>(DEFAULT_CACHE_SIZE * 2);

		// init table from graph metastate based on index

		this->left_table_ = new Table<InTypeLeft, MatchType>(left_meta.delta_filename_, left_meta.pages_,
		                                                     left_meta.btree_pages_, bp, graph_, get_match_left);

		// we also need to set ts for the node, we will use left ts for it, thus right ts will always be 0

		// get reference to corresponding metastate

		this->right_table_ = new Table<InTypeRight, MatchType>(left_meta.delta_filename_, left_meta.pages_,
		                                                       left_meta.btree_pages_, bp, graph_, get_match_right);
	}

	// this function changes
	bool Compute() {
		bool produced = false;
		// compute right_cache against left_cache
		// they are small and hold no indexes so we do it just by nested loop
		while (this->in_cache_left_->HasNext()) {
			Tuple<InTypeLeft> in_left_tuple = this->in_cache_left_->GetNext();
			while (this->in_cache_right_->HasNext()) {
				Tuple<InTypeRight> in_right_tuple = this->in_cache_right_->GetNext();
				// if left and right caches match on data put it into out_cache with new delta

				if (this->Compare(&in_left_tuple.data, &in_right_tuple.data)) {
					Tuple<OutType> out_tuple;
					out_tuple.delta = {std::max(in_left_tuple.delta.ts, in_right_tuple.delta.ts),
					                   in_left_tuple.delta.count * in_right_tuple.delta.count};

					out_tuple.data = this->join_layout_(in_left_tuple.data, in_right_tuple.data);
					out_cache_->Insert(out_tuple);
					produced = true;
				}
			}
		}

		// compute left cache against right table
		while (this->in_cache_left_->HasNext()) {
			Tuple<InTypeLeft> in_left_tuple = this->in_cache_left_->GetNext();
			MatchType match = this->get_match_left_(in_left_tuple.data);

			auto matches = this->right_table_->MatchSearch(match);

			for (auto it = matches.begin(); it != matches.end(); it++) {
				index idx = it->second;
				InTypeRight &right_data = it->first;
				// deltas from right table
				const std::vector<Delta> &right_deltas = this->right_table_->Scan(idx);
				// iterate all deltas of this tuple

				Tuple<OutType> out_tuple;
				out_tuple.data = this->join_layout_(in_left_tuple.data, right_data);
				for (auto &right_delta : right_deltas) {
					out_tuple.delta = {std::max(in_left_tuple.delta.ts, right_delta.ts),
					                   in_left_tuple.delta.count * right_delta.count};
					this->out_cache_->Insert(out_tuple);
					produced = true;
				}
			}
		}

		// compute right cache against left table
		while (this->in_cache_right_->HasNext()) {
			Tuple<InTypeRight> in_right_tuple = this->in_cache_right_->GetNext();
			MatchType match = this->get_match_right_(in_right_tuple.data);
			auto matches = this->left_table_->MatchSearch(match);

			for (auto it = matches.begin(); it != matches.end(); it++) {
				index idx = it->second;
				InTypeLeft &left_data = it->first;
				const std::vector<Delta> &left_deltas = this->left_table_->Scan(idx);
				// iterate all deltas of this tuple

				Tuple<OutType> out_tuple;
				out_tuple.data = this->join_layout_(left_data, in_right_tuple.data);
				for (auto &left_delta : left_deltas) {
					out_tuple.delta = {std::max(in_right_tuple.delta.ts, left_delta.ts),
					                   in_right_tuple.delta.count * left_delta.count};
					this->out_cache_->Insert(out_tuple);
					produced = true;
				}
			}
		}

		this->out_cache_->FinishInserting();

		// insert new deltas from in_caches
		while (this->in_cache_right_->HasNext()) {
			Tuple<InTypeRight> in_right_tuple = this->in_cache_right_->GetNext();
			index idx = this->right_table_->Insert(in_right_tuple.data);
			this->right_table_->InsertDelta(idx, in_right_tuple.delta);
		}
		while (this->in_cache_left_->HasNext()) {
			Tuple<InTypeLeft> in_left_tuple = this->in_cache_left_->GetNext();
			index idx = this->left_table_->Insert(in_left_tuple.data);
			this->left_table_->InsertDelta(idx, in_left_tuple.delta);
		}

		// clean in_caches
		this->in_node_left_->CleanCache();
		this->in_node_right_->CleanCache();

		if (this->compact_) {
			this->Compact();
		}

		// periodically call garbage collector
		if (this->gb_settings_.use_garbage_collector && this->next_clean_ts_ < this->ts_) {
			this->left_table_->GarbageCollect(this->ts_ - this->gb_settings_.delete_age_,
			                                  this->gb_settings_.remove_zeros_only);
			this->right_table_->GarbageCollect(this->ts_ - this->gb_settings_.delete_age_,
			                                   this->gb_settings_.remove_zeros_only);
			this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;
		}

		return produced;
	}

	~JoinNode() {
		delete this->out_cache_;
		delete this->left_table_;
		delete this->right_table_;
	}

	Cache<OutType> *Output() {
		this->out_count++;
		return this->out_cache_;
	}

	void CleanCache() {
		this->clean_count.fetch_add(1);
		if (this->clean_count.load() == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count.exchange(0);
		}
	}

	void UpdateTimestamp() {
		timestamp ts = this->OldestTsToKeep();
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->compact_ = true;
			this->ts_ = ts;
			this->in_node_left_->UpdateTimestamp();
			this->in_node_right_->UpdateTimestamp();
		}
	}

private:
	inline bool Compare(InTypeLeft *left_data, InTypeRight *right_data) {
		MatchType left_match = this->get_match_left_(*left_data);
		MatchType right_match = this->get_match_right_(*right_data);
		return !std::memcmp(&left_match, &right_match, sizeof(MatchType));
	}

	void Compact() {
		this->left_table_->MergeDelta(this->ts_);
		this->right_table_->MergeDelta(this->ts_);
	}

	std::function<MatchType(const InTypeLeft &)> get_match_left_;
	std::function<MatchType(const InTypeRight &)> get_match_right_;
	std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout_;

	Graph *graph_;

	TypedNode<InTypeLeft> *in_node_left_;
	TypedNode<InTypeRight> *in_node_right_;

	Table<InTypeLeft, MatchType> *left_table_;
	Table<InTypeRight, MatchType> *right_table_;

	Cache<InTypeLeft> *in_cache_left_;
	Cache<InTypeRight> *in_cache_right_;

	Cache<OutType> *out_cache_;
	int out_count = 0;
	std::atomic<int> clean_count {0};

	GarbageCollectSettings &gb_settings_;
	timestamp next_clean_ts_;

	MetaState &left_meta_;
	MetaState &right_meta_;

	// timestamp will be used to track valid tuples
	// after update propagate it to input nodes
	bool compact_ = false;
};

template <typename InType, typename MatchType, typename OutType>
class AggregateByNode : public TypedNode<OutType> {

public:
	AggregateByNode(TypedNode<InType> *in_node,
	                // count corresponds to count from delta, outtype is current aggregated OutType from rest of InType
	                // matched tuples
	                std::function<OutType(const InType &, int, const OutType &, bool)> aggr_fun,
	                std::function<MatchType(const InType &)> get_match, Graph *graph, BufferPool *bp,
	                GarbageCollectSettings &gb_settings, MetaState &meta, index table_index)
	    : aggr_fun_ {aggr_fun}, get_match_ {get_match}, graph_ {graph}, in_node_ {in_node},
	      in_cache_ {in_node->Output()}, gb_settings_ {gb_settings}, meta_ {meta},
	      table_index_ {table_index}, TypedNode<OutType> {in_node->GetFrontierTs()}, previous_ts_ {meta.previous_ts_},
	      recompute_indexes_ {meta.recompute_idexes_}, not_emited_ {meta.not_emited_} {

		this->ts_ = get_current_timestamp();
		this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;

		// there will be single tuple emited at once probably, if not it will get
		// resized so chill
		this->out_cache_ = new Cache<OutType>(2);

		// init table from graph metastate based on index
		this->table_ =
		    new Table<InType, MatchType>(meta.delta_filename_, meta.pages_, meta.btree_pages_, bp, graph_, get_match);
	}

	~AggregateByNode() {
		delete out_cache_;
		delete table_;
	}

	Cache<OutType> *Output() {
		this->out_count++;
		return this->out_cache_;
	}

	void CleanCache() {
		this->clean_count.fetch_add(1);
		if (this->clean_count.load() == this->out_count) {
			this->out_cache_->Clean();
			this->clean_count.exchange(0);
		}
	}

	// output should be single tuple with updated values for different times
	// so what we can do there? if we emit new count as part of value, then it
	// will be treated as separate tuple if we emit new value as part of delta it
	// also will be wrong what if we would do : insert previous value with count
	// -1 and insert current with count 1 at the same time then old should get
	// discarded
	bool Compute() {
		bool produced = false;
		// first insert all new data from cache to table
		while (this->in_cache_->HasNext()) {
			const Tuple<InType> in_tuple = this->in_cache_->GetNext();

			index idx = this->table_->Insert(in_tuple.data);
			bool was_present = this->table_->InsertDelta(idx, in_tuple.delta);
			recompute_indexes_.insert(idx);

			if (!was_present) {
				not_emited_.insert(idx);
			}
		}

		if (this->compact_) {
			// delta_count for oldest keept version, from this we can deduce what tuples
			// to emit;

			//  out node will always have count of each tuple as either 0 or 1

			// insert and delete index in out edge cache, for this match type

			for (const auto &idx : recompute_indexes_) {
				InType data = this->table_->Get(idx);
				MatchType match = this->get_match_(data);

				Tuple<OutType> insert_tpl;
				insert_tpl.delta.count = 1;
				insert_tpl.delta.ts = this->ts_;

				Tuple<OutType> delete_tpl;
				delete_tpl.delta.count = -1;
				delete_tpl.delta.ts = this->ts_;

				auto matches = this->table_->MatchSearch(match);

				bool first = true;
				bool delete_any = false;
				bool first_delete = true;

				for (auto it = matches.begin(); it != matches.end(); it++) {
					InType &matched_data = it->first;
					// deltas from right table
					const std::vector<Delta> &deltas = this->table_->Scan(idx);

					// iterate all deltas of this tuple
					int delete_count = 0;
					int count = 0;
					for (const auto &delta : deltas) {
						// we might need to emit delete for previous version of this tuple
						// previous version of this tuple existed if any of matches was already emited
						// then we sum deltas older/equal to previous compaction ts
						if (delta.ts <= this->previous_ts_ && !not_emited_.contains(it->second)) {
							delete_count += delta.count;
							delete_any = true;
						}
						if (delta.ts > this->ts_) {
							break;
						}
						count += delta.count;
					}
					if (delete_count > 0) {
						delete_tpl.data = this->aggr_fun_(matched_data, delete_count, delete_tpl.data, first_delete);
						first_delete = false;
					}
					/* for first time computation we need to start with init value, after that we recompute based on
					 * previous result */
					insert_tpl.data = this->aggr_fun_(matched_data, count, insert_tpl.data, first);
					first = false;
				}

				this->out_cache_->Insert(insert_tpl);
				if (delete_any) {
					this->out_cache_->Insert(delete_tpl);
					produced = true;
				}
			}

			this->Compact(); // after this iteration oldest version keep will be one with timestamp of current compact
			this->compact_ = false;

			recompute_indexes_ = {};
			not_emited_ = {};
		}

		this->out_cache_->FinishInserting();
		this->in_node_->CleanCache();

		// periodically call garbage collector
		if (this->gb_settings_.use_garbage_collector && this->next_clean_ts_ < this->ts_) {
			this->table_->GarbageCollect(this->ts_ - this->gb_settings_.delete_age_,
			                             this->gb_settings_.remove_zeros_only);
			this->next_clean_ts_ = this->ts_ + this->gb_settings_.clean_freq_;
		}

		return produced;
	}

	void UpdateTimestamp() {
		timestamp ts = this->OldestTsToKeep();
		if (this->ts_ + this->frontier_ts_ < ts) {
			this->previous_ts_ = this->ts_;
			this->ts_ = ts;
			this->compact_ = true;
			this->in_node_->UpdateTimestamp();
		}
	}

private:
	// let's store full version data and not deltas in this node, so we can just
	// discard old versions
	void Compact() {
		// compact all the way to previous version, we need it to to emit delete later
		this->table_->MergeDelta(this->previous_ts_);
	}

	std::function<OutType(const InType &, int count, const OutType &, bool first)> aggr_fun_;
	std::function<MatchType(const InType &)> get_match_;

	Graph *graph_;

	TypedNode<InType> *in_node_;

	Table<InType, MatchType> *table_;
	index table_index_;

	Cache<InType> *in_cache_;
	Cache<OutType> *out_cache_;
	int out_count = 0;
	std::atomic<int> clean_count {0};

	GarbageCollectSettings &gb_settings_;
	timestamp next_clean_ts_;

	MetaState &meta_;

	// timestamp will be used to track valid tuples
	// after update propagate it to input nodes
	bool compact_ = false;
	timestamp &previous_ts_;

	std::set<index> &recompute_indexes_;
	// list of indexes that were not emited yet
	std::set<index> &not_emited_;
};

} // namespace AliceDB
#endif
