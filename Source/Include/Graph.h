#ifndef ALICEDBGRAPH
#define ALICEDBGRAPH

#include "Common.h"
#include "Node.h"

#include <functional>
#include <list>
#include <set>
#include <stack>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace AliceDB {

enum class NodeState { PROCESSED, NOT_PROCESSED, PROCESSING };

/**
 * @brief Graph of relational algebra processing nodes
 * where operators such as Filter etc are wrappers around Node with type inference,
 * and automatically keep track of node inputs and graph they belong to
 *
 * Graph is started by calling Process function, after that no new node can be added
 */

struct MetaState {
	std::vector<index> pages_;
	std::vector<index> btree_pages_;
	std::vector<index> match_btree_pages_;
};

class Graph {
public:
	Graph(std::string graph_filename) {
	}

	~Graph() {
		// update graph metadatafile
		// ok and what do we actually need to store there?
		// like connections and node lists is actually stored in object itself(in code)
		// what we need to store instead is for each node it's metadata info such that each node will be called with
		// right args for Table

		// ok tables will hold reference to the catalog, so i think we need to just simply pass this pointer to the Node
		// that is being created

		// so we just need to write all this metadata to some files
	}

	// Tables call this on destruction to update info
	void UpdateTableMetadata(MetaState &table_meta, int table_index) {
		this->tables_metadata_[table_index] = std::move(table_meta);
	}

	// Node creations

	template <typename P>
	auto Source(P *prod, timestamp frontier_ts, int duration_us = 500) -> TypedNode<typename P::value_type> * {
		this->check_running();
		using Type = typename P::value_type;
		// source has 1 table
		index idx = this->next_table_index_;
		this->next_table_index_++;
		/** @todo
		    now we can read metadata from our graph state read from file ... and pass it into node constructor,
		    we also should pass indexes so that we will be able to update state when closing the graph,
		    repeat the same thing for other nodes, so that's kinda solved
		*/

		auto *source_node = new SourceNode<Type>(prod, frontier_ts, duration_us);
		all_nodes_.insert(static_cast<Node *>(source_node));
		sources_.insert(static_cast<Node *>(source_node));
		source_node->set_graph(this);
		return source_node;

		for (void *table : tables) {
			this->table_to_index_[table] = idx;
		}
	}

	template <typename N>
	auto View(N *in_node) -> TypedNode<typename N::value_type> * {
		this->check_running();
		using InType = typename N::value_type;
		TypedNode<InType> *sink = new SinkNode<InType>(in_node);
		make_edge(static_cast<Node *>(in_node), static_cast<Node *>(sink));
		all_nodes_.insert(static_cast<Node *>(sink));
		sinks_.insert(static_cast<Node *>(sink));
		sink->set_graph(this);
		return sink;
	}

	template <typename F, typename N>
	auto Filter(F condition, N *in_node) -> TypedNode<typename N::value_type> * {
		this->check_running();
		using Type = typename N::value_type;
		auto *filter = new FilterNode<Type>(in_node, condition);
		all_nodes_.insert(static_cast<Node *>(filter));
		make_edge(static_cast<Node *>(in_node), static_cast<Node *>(filter));
		filter->set_graph(this);
		return filter;
	}

	template <typename F, typename N>
	auto Projection(F projection_function, N *in_node)
	    -> TypedNode<std::invoke_result_t<F, const typename N::value_type &>> * {
		this->check_running();
		using InType = typename N::value_type;
		using OutType = std::invoke_result_t<F, const InType &>;
		TypedNode<OutType> *projection = new ProjectionNode<InType, OutType>(in_node, projection_function);
		all_nodes_.insert(static_cast<Node *>(projection));
		make_edge(static_cast<Node *>(in_node), static_cast<Node *>(projection));
		projection->set_graph(this);
		return projection;
	}

	template <typename N>
	auto Distinct(N *in_node) -> TypedNode<typename N::value_type> * {
		this->check_running();
		using Type = typename N::value_type;
		auto *distinct = new DistinctNode<Type>(in_node);
		all_nodes_.insert(static_cast<Node *>(distinct));
		make_edge(static_cast<Node *>(in_node), static_cast<Node *>(distinct));
		distinct->set_graph(this);
		return distinct;
	}

	template <typename N>
	auto Union(N *in_node_left, N *in_node_right) -> TypedNode<typename N::value_type> * {
		this->check_running();
		using Type = typename N::value_type;
		TypedNode<Type> *plus = new PlusNode<Type>(in_node_left, in_node_right, false);
		all_nodes_.insert(static_cast<Node *>(plus));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(plus));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(plus));
		plus->set_graph(this);
		return Distinct(plus);
	}

	template <typename N>
	auto Except(N *in_node_left, N *in_node_right) -> TypedNode<typename N::value_type> * {
		this->check_running();
		using Type = typename N::value_type;
		TypedNode<Type> *plus = new PlusNode<Type>(in_node_left, in_node_right, true);
		all_nodes_.insert(static_cast<Node *>(plus));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(plus));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(plus));
		plus->set_graph(this);
		return Distinct(plus);
	}

	template <typename N>
	auto Intersect(N *in_node_left, N *in_node_right) -> TypedNode<typename N::value_type> * {
		this->check_running();
		using Type = typename N::value_type;
		TypedNode<Type> *intersect = new IntersectNode<Type>(in_node_left, in_node_right);
		all_nodes_.insert(static_cast<Node *>(intersect));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(intersect));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(intersect));
		intersect->set_graph(this);
		return Distinct(intersect);
	}

	// crossjoin deduces outtype from join_layout and input node types
	template <typename F, typename NL, typename NR>
	auto CrossJoin(F join_layout, NL *in_node_left, NR *in_node_right)
	    -> TypedNode<std::invoke_result_t<F, const typename NL::value_type &, const typename NR::value_type &>> * {
		this->check_running();
		using InTypeLeft = typename NL::value_type;
		using InTypeRight = typename NR::value_type;
		using OutType = std::invoke_result_t<F, const InTypeLeft &, const InTypeRight &>;
		TypedNode<OutType> *cross_join =
		    new CrossJoinNode<InTypeLeft, InTypeRight, OutType>(in_node_left, in_node_right, join_layout);
		all_nodes_.insert(static_cast<Node *>(cross_join));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(cross_join));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(cross_join));
		cross_join->set_graph(this);
		return cross_join;
	}

	// join deduces matchType get_match_left, get_match_right, and mutType from join_layout
	template <typename F_left, typename F_right, typename F_join, typename NL, typename NR>
	auto Join(F_left get_match_left, F_right get_match_right, F_join join_layout, NL *in_node_left, NR *in_node_right)
	    -> TypedNode<std::invoke_result_t<F_join, const typename NL::value_type &, const typename NR::value_type &>> * {
		this->check_running();
		using InTypeLeft = typename NL::value_type;
		using InTypeRight = typename NR::value_type;
		using MatchTypeLeft = std::invoke_result_t<F_left, const InTypeLeft &>;
		using MatchTypeRight = std::invoke_result_t<F_right, const InTypeRight &>;
		static_assert(std::is_same_v<MatchTypeLeft, MatchTypeRight>, "Left/Right match keys differ");
		using MatchType = MatchTypeLeft;
		using OutType = std::invoke_result_t<F_join, const InTypeLeft &, const InTypeRight &>;
		TypedNode<OutType> *join = new JoinNode<InTypeLeft, InTypeRight, MatchType, OutType>(
		    in_node_left, in_node_right, get_match_left, get_match_right, join_layout);
		all_nodes_.insert(static_cast<Node *>(join));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(join));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(join));
		join->set_graph(this);
		return join;
	}

	template <typename T>
	struct function_traits : function_traits<decltype(&T::operator())> {};

	template <typename ClassType, typename ReturnType, typename... Args>
	struct function_traits<ReturnType (ClassType::*)(Args...) const> {
		using return_type = ReturnType;
		using arguments_tuple = std::tuple<Args...>;
	};

	template <typename F_aggr, typename F_getmatch, typename N>
	auto AggregateBy(F_aggr aggr_fun, F_getmatch get_match, N *in_node) -> TypedNode<
	    std::remove_reference_t<std::tuple_element_t<1, typename function_traits<F_aggr>::arguments_tuple>>> * {
		this->check_running();
		using InType = typename N::value_type;

		// extract parameter types from aggr_fun
		using AggrTraits = function_traits<F_aggr>;
		using AggrArgs = typename AggrTraits::arguments_tuple;

		using FirstAggrArg = std::tuple_element_t<0, AggrArgs>;
		using SecondAggrArg = std::tuple_element_t<1, AggrArgs>;

		using OutType = std::remove_reference_t<SecondAggrArg>;
		using MatchType = std::invoke_result_t<F_getmatch, const InType &>;

		TypedNode<OutType> *aggr = new AggregateByNode<InType, MatchType, OutType>(in_node, aggr_fun, get_match);
		this->all_nodes_.insert(static_cast<Node *>(aggr));
		this->make_edge(static_cast<Node *>(in_node), static_cast<Node *>(aggr));
		aggr->set_graph(this);
		return aggr;
	}

	// Node processing

	// return next node of the graph to be processed
	/**
	 * @brief set next_node to next node to be processed
	 * @return true on succes, false when there is no nodes at the current time to be processed
	 */
	bool GetNext(Node **next_node) {
		this->graph_latch_.lock();

		// this means we can reset levels
		if (this->current_level_ >= this->levels_.size() - 1 && AllProcesed()) {
			// great all processed, reet state ad start from beginning
			this->current_level_ = 0;
			this->current_index_ = 0;
			for (const auto &node : this->all_nodes_) {
				this->nodes_state_[node] = NodeState::NOT_PROCESSED;
			}
		}
		// we assigned all nodes at highest level but some were not processed yet, return false
		if (this->current_level_ > this->levels_.size()) {
			return false;
		}

		*next_node = this->levels_[current_level_][current_index_];
		for (const auto &node : this->node_dependencies_[*next_node]) {
			if (this->nodes_state_[node] != NodeState::PROCESSED) {
				*next_node = nullptr;
				return false;
			}
		}

		// all nodes at current level were assigned, graduate to new level
		current_index_++;
		if (current_index_ >= this->levels_[current_level_].size()) {
			current_index_ = 0;
			current_level_++;
		}

		this->SetState(*next_node, NodeState::PROCESSING);
		this->graph_latch_.unlock();

		return true;
	}

	/**
	 * @return true if all nodes were processed in current iteration
	 */
	bool AllProcesed() {
		// all nodes are processed when all sink nodes are processed
		for (const auto &node : this->sinks_) {
			if (this->nodes_state_[node] != NodeState::PROCESSED) {
				return false;
			}
		}
		return true;
	}

	void SetState(Node *node, NodeState state) {
		this->nodes_state_[node] = state;
	}

	/**
	 * @brief
	 * Created when all nodes are appended to graph, and processing can be started,
	 * this is called by WorkerPool->Start(Graph *g)
	 */
	void Start() {
		// after calling process once graph no longer will accept adding new nodes
		this->is_graph_running_ = true;
		if (this->topo_graph_.empty()) {
			this->topo_sort();
		}
	}

private:
	/*
	    This datastructure is smiliar to standard topological order
	    list of graph except it's leveled.
	    Node at level N can depend on any node on level N-k where k <= N,
	    but not on any node at level N+i where i >= 0
  */
	void TopoLevelList() {
		std::unordered_map<Node *, int> node_levels;
		// list of all dependencies for given node

		// iterate topo_list
		// set level 0 to first node, and set level 1 to all outputs
		// for all out set level + 1 as level,
		int max_level = 0;

		for (auto node : this->topo_graph_) {
			// If the node has no dependencies, it should be at level 0
			// Since it's a topological sort, nodes are processed after their dependencies
			// So the current level of 'node' has already been determined

			// If the node is not in node_levels, it means it has no dependencies
			// because otherwise it's dependency would be in a topolist before
			if (!node_levels.contains(node)) {
				node_levels[node] = 0;
				this->node_dependencies_[node] = {};
			}

			int current_level = node_levels[node];

			// Iterate through all children (nodes that depend on the current node)
			for (auto child : out_edges_[node]) {
				// Assign the child to the next level if necessary
				if (!node_levels.contains(child)) {
					node_levels[child] = current_level + 1;
				} else {
					node_levels[child] =
					    node_levels[child] > current_level + 1 ? node_levels[child] : current_level + 1;
				}

				// Update the maximum level found so far
				if (node_levels[child] > max_level) {
					max_level = node_levels[child];
				}

				this->append_parent_dependencies(child, node);
			}
		}

		// Organize nodes into levels
		this->levels_.resize(max_level + 1);
		for (const auto &[node, level] : node_levels) {
			this->levels_[level].push_back(node);
		}
	}

	void append_parent_dependencies(Node *child_node, Node *parent_node) {
		if (!this->node_dependencies_.contains(child_node)) {
			this->node_dependencies_[child_node] = {};
		}
		std::set<Node *> &child_deps = this->node_dependencies_[child_node];
		std::set<Node *> &parent_deps = this->node_dependencies_[parent_node];
		child_deps.insert(parent_node);
		for (const auto &dep : parent_deps) {
			child_deps.insert(dep);
		}
	}

	void topo_sort() {
		std::set<Node *> visited;
		std::stack<Node *> stack;

		// for each node:
		for (auto it = all_nodes_.begin(); it != all_nodes_.end(); it++) {
			Node *current = *it;

			if (visited.contains(current)) {
				continue;
			}

			std::set<Node *> current_run = {};

			// if visit return's 1 there was a cycle,
			// since those are not allowed we throw here
			if (visit(current, visited, stack, current_run)) {
				throw std::runtime_error("[Error] graph contains cycle");
			}
		}

		// save this topo_graph as list
		std::list<Node *> topo_order;
		while (!stack.empty()) {
			topo_graph_.push_back(stack.top());
			stack.pop();
		}

		// create vector of list of nodes grouped by dependency level
		this->TopoLevelList();
	}

	bool visit(Node *current, std::set<Node *> &visited, std::stack<Node *> &stack, std::set<Node *> &current_run) {
		bool has_cycle = 0;
		if (current_run.contains(current)) {
			return true;
		}
		if (visited.contains(current)) {
			return false;
		}

		visited.insert(current);
		current_run.insert(current);
		if (out_edges_.contains(current)) {
			for (Node *neighbour : out_edges_[current]) {
				has_cycle |= visit(neighbour, visited, stack, current_run);
			}
		}
		stack.emplace(current);
		return has_cycle;
	}

	void make_edge(Node *in_node, Node *out_node) {
		if (out_edges_.contains(in_node)) {
			out_edges_[in_node].emplace_front(out_node);
		} else {
			out_edges_[in_node] = {out_node};
		}
	}

	void check_running() {
		if (this->is_graph_running_) {
			throw std::runtime_error("Graph is already running, can't add new nodes to it\n");
		}
	}

	std::unordered_map<Node *, std::list<Node *>> out_edges_;

	std::unordered_set<Node *> all_nodes_;

	std::set<Node *> sinks_;
	std::set<Node *> sources_;

	// List of nodes representing topological orders
	std::list<Node *> topo_graph_;
	// topolist but leveled, where node at level n can only depends on nodes on levels < n
	std::vector<std::vector<Node *>> levels_;
	// map of all dependencies for given node
	std::unordered_map<Node *, std::set<Node *>> node_dependencies_;

	std::unordered_map<Node *, NodeState> nodes_state_;

	std::mutex graph_latch_;

	bool is_graph_running_ = false;

	int current_level_ = 0;
	int current_index_ = 0;

	// for persisten storage, each node might store from 0 to 2 tables
	int next_table_index_ = 0;
	std::unordered_map<index, MetaState> tables_metadata_;
};

} // namespace AliceDB

#endif
