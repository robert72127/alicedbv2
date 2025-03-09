#ifndef ALICEDBGRAPH
#define ALICEDBGRAPH

#include "Common.h"
#include "Node.h"

#include <filesystem>
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
 * Graph is started by calling Start method, after that no new node can be added
 */
class Graph {
public:
	/** @brief  create graph instance with, read metadata from graph_directory if it exists,
	 * load garbage collector settings that will be applied for all nodes */
	Graph(std::filesystem::path graph_directory, BufferPool *bp, GarbageCollectSettings &gb_settings)
	    : graph_directory_ {graph_directory}, bp_ {bp}, gb_settings_ {gb_settings} {

		out_edges_ = {};
		in_edges_ = {};

		// graph file format:
		/*
		INDEX <idx>
		PREVIOUS_TIMESTAMP <ts>
		RECOMPUTE_INDEXES <recompute indexes ...>
		ENDRECOMPUTEINDEXES
		NOTEMITED <not emited indexes ...>
		ENDNOTEMITED
		DELTAFILENAME <metafilename>
		PAGES <page_idx ....>
		ENDPAGES
		BTREEPAGES <page_idx ...>
		ENDBTREEPAGES <page_idx...>
		ENDINDEX
		*/
		std::string graph_metadata_file = this->graph_directory_ / "graph_metadata";

		std::ifstream input_stream(graph_metadata_file);
		if (!input_stream) {
			// ok file doesn't exists yet, but it will after our first processing, so we can early return from
			// constructor here
			return;
		}

		std::string token;
		// Continue reading until end-of-file.
		while (input_stream >> token) {
			// Each record must start with "INDEX"
			if (token != "INDEX") {
				throw std::runtime_error("Expected INDEX, got: " + token);
			}
			// Read the table index.
			index table_idx;
			if (!(input_stream >> table_idx)) {
				throw std::runtime_error("Error reading table index value");
			}

			MetaState meta;

			meta.table_idx_ = table_idx;

			// EXPECT TIMESTAMP <ts>
			input_stream >> token;
			if (token != "PREVIOUS_TIMESTAMP") {
				throw std::runtime_error("Expected TIMESTAMP, got: " + token);
			}
			input_stream >> meta.previous_ts_;

			// Expect the "RECOMPUTEINDEXES" block.
			input_stream >> token;
			if (token != "RECOMPUTEINDEXES") {
				throw std::runtime_error("Expected RECOMPUTEINDEXES, got: " + token);
			}

			while (input_stream >> token && token != "ENDRECOMPUTEINDEXES") {
				std::istringstream iss(token);
				index idx;
				if (!(iss >> idx)) {
					throw std::runtime_error("Error index: " + token);
				}
				meta.recompute_idexes_.insert(idx);
			}
			// We expect token == "ENDRECOMPUTEINDEXES" here.
			if (token != "ENDRECOMPUTEINDEXES") {
				throw std::runtime_error("Did not find ENDRECOMPUTEINDEXES token");
			}

			// Expect the "NOTEMITED" block.
			input_stream >> token;
			if (token != "NOTEMITED") {
				throw std::runtime_error("Expected NOTEMITED, got: " + token);
			}

			while (input_stream >> token && token != "ENDNOTEMITED") {
				std::istringstream iss(token);
				index idx;
				if (!(iss >> idx)) {
					throw std::runtime_error("Error index: " + token);
				}
				meta.recompute_idexes_.insert(idx);
			}
			// We expect token == "ENDRECOMPUTEINDEXES" here.
			if (token != "ENDNOTEMITED") {
				throw std::runtime_error("Did not find ENDNOTEMITED token");
			}

			// EXPECT DELTAFILENAME <deltafilename>
			input_stream >> token;
			if (token != "DELTAFILENAME") {
				throw std::runtime_error("Expected DELTAFILENAME, got: " + token);
			}
			input_stream >> meta.delta_filename_;

			// Expect the "PAGES" block.
			input_stream >> token;
			if (token != "PAGES") {
				throw std::runtime_error("Expected PAGES, got: " + token);
			}
			// Read all page indices until the ENDPAGES token.
			while (input_stream >> token && token != "ENDPAGES") {
				std::istringstream iss(token);
				index page;
				if (!(iss >> page)) {
					throw std::runtime_error("Error reading page index: " + token);
				}
				meta.pages_.push_back(page);
			}
			// We expect token == "ENDPAGES" here.
			if (token != "ENDPAGES") {
				throw std::runtime_error("Did not find ENDPAGES token");
			}

			// Next, expect the "BTREEPAGES" block.
			input_stream >> token;
			if (token != "BTREEPAGES") {
				throw std::runtime_error("Expected BTREEPAGES, got: " + token);
			}
			// Read all btree page indices until the ENDBTREEPAGES token.
			while (input_stream >> token && token != "ENDBTREEPAGES") {
				std::istringstream iss(token);
				index btree_page;
				if (!(iss >> btree_page)) {
					throw std::runtime_error("Error reading btree page index: " + token);
				}
				meta.btree_pages_.push_back(btree_page);
			}
			if (token != "ENDBTREEPAGES") {
				throw std::runtime_error("Did not find ENDBTREEPAGES token");
			}

			// Finally, expect the "ENDINDEX" token.
			input_stream >> token;
			if (token != "ENDINDEX") {
				throw std::runtime_error("Expected ENDINDEX, got: " + token);
			}

			// Save the parsed MetaState into the unordered_map.
			tables_metadata_[table_idx] = meta;
		}
	}

	/** Write metadata about graph to file and shut it down */
	~Graph() {

		/** call destructor on nodes */
		for (const auto *Node : all_nodes_) {
			delete Node;
		}

		// update graph metadatafile
		// ok and what do we actually need to store there?
		// like connections and node lists is actually stored in object itself(in code)
		// what we need to store instead is for each node it's metadata info such that each node will be called with
		// right args for Table

		// ok tables will hold reference to the catalog, so i think we need to just simply pass this pointer to the Node
		// that is being created

		// so we just need to write all this metadata to some files

		std::string graph_metadata_file = this->graph_directory_ / "graph_metadata";
		std::ofstream output_stream(graph_metadata_file, std::ios::trunc);

		std::string tmp_filename = graph_metadata_file + "_tmp";

		for (const auto &entry : tables_metadata_) {
			index table_idx = entry.first;
			const MetaState &meta = entry.second;
			output_stream << "INDEX " << table_idx << "\n";

			output_stream << "PREVIOUS_TIMESTAMP " << meta.previous_ts_ << "\n";

			output_stream << "RECOMPUTEINDEXES";
			for (const auto &idx : meta.recompute_idexes_) {
				output_stream << " " << idx;
			}
			output_stream << "\nENDRECOMPUTEINDEXES\n";

			output_stream << "NOTEMITED";
			for (const auto &idx : meta.not_emited_) {
				output_stream << " " << idx;
			}
			output_stream << "\nENDNOTEMITED\n";

			output_stream << "DELTAFILENAME " << meta.delta_filename_ << "\n";

			output_stream << "PAGES";
			for (const auto &page : meta.pages_) {
				output_stream << " " << page;
			}
			output_stream << "\nENDPAGES\n";

			output_stream << "BTREEPAGES";
			for (const auto &btree_page : meta.btree_pages_) {
				output_stream << " " << btree_page;
			}
			output_stream << "\nENDBTREEPAGES\n";

			output_stream << "ENDINDEX\n";
		}
	}

	/* All metadata about tables is owned by graph
	 graph. nodes will just hold references to it's own meta state based on index */
	MetaState &GetTableMetadata(index table_idx) {
		return this->tables_metadata_[table_idx];
	}

	// Stateless  node,  responsible for reading data from producer, and passing it to the system
	template <typename Type>
	auto Source(ProducerType prod_type, const std::string &prod_source,
	            std::function<bool(std::istringstream &, Type *)> parse, timestamp frontier_ts, int duration_us = 500)
	    -> TypedNode<Type> * {
		this->check_running();
		TypedNode<Type> *Source =
		    new SourceNode<Type>(prod_type, prod_source, parse, frontier_ts, duration_us = 500, this);
		all_nodes_.insert(static_cast<Node *>(Source));
		sources_.insert(static_cast<Node *>(Source));
		return Source;
	}

	// Allows for storing output state of any node
	template <typename N>
	auto View(N *in_node) -> TypedNode<typename N::value_type> * {
		this->check_running();

		index table_index = this->maybe_create_table();

		using InType = typename N::value_type;
		TypedNode<InType> *sink = new SinkNode<InType>(in_node, this, this->bp_, this->gb_settings_,
		                                               this->GetTableMetadata(table_index), table_index);
		make_edge(static_cast<Node *>(in_node), static_cast<Node *>(sink));
		all_nodes_.insert(static_cast<Node *>(sink));
		sinks_.insert(static_cast<Node *>(sink));
		return sink;
	}

	// stateless node that filters tuples based on condition applied to it
	template <typename F, typename N>
	auto Filter(F condition, N *in_node) -> TypedNode<typename N::value_type> * {
		this->check_running();
		using Type = typename N::value_type;
		auto *filter = new FilterNode<Type>(in_node, condition, this);
		all_nodes_.insert(static_cast<Node *>(filter));
		make_edge(static_cast<Node *>(in_node), static_cast<Node *>(filter));
		return filter;
	}

	// stateless node that creates output tuples from input tuples using projection function
	template <typename F, typename N>
	auto Projection(F projection_function, N *in_node)
	    -> TypedNode<std::invoke_result_t<F, const typename N::value_type &>> * {
		this->check_running();
		using InType = typename N::value_type;
		using OutType = std::invoke_result_t<F, const InType &>;
		TypedNode<OutType> *projection = new ProjectionNode<InType, OutType>(in_node, projection_function, this);
		all_nodes_.insert(static_cast<Node *>(projection));
		make_edge(static_cast<Node *>(in_node), static_cast<Node *>(projection));
		return projection;
	}

	// Statefull node that keeps track whether current tuple count is positive or negative for each tuple
	template <typename N>
	auto Distinct(N *in_node) -> TypedNode<typename N::value_type> * {
		this->check_running();

		index table_index = this->maybe_create_table();

		using Type = typename N::value_type;
		auto *distinct = new DistinctNode<Type>(in_node, this, this->bp_, this->gb_settings_,
		                                        this->GetTableMetadata(table_index), table_index);
		all_nodes_.insert(static_cast<Node *>(distinct));
		make_edge(static_cast<Node *>(in_node), static_cast<Node *>(distinct));
		return distinct;
	}

	// Creates stateless union node that unions inputs from left and right node, after that creates distinct node as its
	// output
	template <typename N>
	auto Union(N *in_node_left, N *in_node_right) -> TypedNode<typename N::value_type> * {
		this->check_running();
		using Type = typename N::value_type;
		TypedNode<Type> *plus = new PlusNode<Type>(in_node_left, in_node_right, false, this);
		all_nodes_.insert(static_cast<Node *>(plus));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(plus));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(plus));
		return Distinct(plus);
	}

	// Creates stateless except node that computes difference of inputs from left and right node, after that creates
	// distinct node as its output
	template <typename N>
	auto Except(N *in_node_left, N *in_node_right) -> TypedNode<typename N::value_type> * {
		this->check_running();
		using Type = typename N::value_type;
		TypedNode<Type> *plus = new PlusNode<Type>(in_node_left, in_node_right, true, this);
		all_nodes_.insert(static_cast<Node *>(plus));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(plus));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(plus));
		return Distinct(plus);
	}

	// Creates statefull node that computes intersection of inputs from left and right node, after that creates distinct
	// node as its output
	template <typename N>
	auto Intersect(N *in_node_left, N *in_node_right) -> TypedNode<typename N::value_type> * {
		this->check_running();

		index left_table_index = this->maybe_create_table();
		index right_table_index = this->maybe_create_table();

		using Type = typename N::value_type;
		TypedNode<Type> *intersect = new IntersectNode<Type>(
		    in_node_left, in_node_right, this, this->bp_, this->gb_settings_, this->GetTableMetadata(left_table_index),
		    this->GetTableMetadata(right_table_index), left_table_index, right_table_index);
		all_nodes_.insert(static_cast<Node *>(intersect));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(intersect));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(intersect));

		return Distinct(intersect);
	}

	// Statefull crossproduct node
	template <typename F, typename NL, typename NR>
	auto CrossJoin(F join_layout, NL *in_node_left, NR *in_node_right)
	    -> TypedNode<std::invoke_result_t<F, const typename NL::value_type &, const typename NR::value_type &>> * {
		this->check_running();

		index left_table_index = this->maybe_create_table();
		index right_table_index = this->maybe_create_table();

		using InTypeLeft = typename NL::value_type;
		using InTypeRight = typename NR::value_type;
		using OutType = std::invoke_result_t<F, const InTypeLeft &, const InTypeRight &>;

		TypedNode<OutType> *cross_join = new CrossJoinNode<InTypeLeft, InTypeRight, OutType>(
		    in_node_left, in_node_right, join_layout, this, this->bp_, this->gb_settings_,
		    this->GetTableMetadata(left_table_index), this->GetTableMetadata(right_table_index), left_table_index,
		    right_table_index);
		all_nodes_.insert(static_cast<Node *>(cross_join));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(cross_join));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(cross_join));

		return cross_join;
	}

	// Statefull join node join deduces matchType get_match_left, get_match_right, and mutType from join_layout
	// get match functions are responsible for accesing matched fields from left and right inputs, join layout
	// specify layout of the output
	template <typename F_left, typename F_right, typename F_join, typename NL, typename NR>
	auto Join(F_left get_match_left, F_right get_match_right, F_join join_layout, NL *in_node_left, NR *in_node_right)
	    -> TypedNode<std::invoke_result_t<F_join, const typename NL::value_type &, const typename NR::value_type &>> * {
		this->check_running();

		index left_table_index = this->maybe_create_table();
		index right_table_index = this->maybe_create_table();

		using InTypeLeft = typename NL::value_type;
		using InTypeRight = typename NR::value_type;
		using MatchTypeLeft = std::invoke_result_t<F_left, const InTypeLeft &>;
		using MatchTypeRight = std::invoke_result_t<F_right, const InTypeRight &>;
		static_assert(std::is_same_v<MatchTypeLeft, MatchTypeRight>, "Left/Right match keys differ");
		using MatchType = MatchTypeLeft;
		using OutType = std::invoke_result_t<F_join, const InTypeLeft &, const InTypeRight &>;
		TypedNode<OutType> *join = new JoinNode<InTypeLeft, InTypeRight, MatchType, OutType>(
		    in_node_left, in_node_right, get_match_left, get_match_right, join_layout, this, this->bp_,
		    this->gb_settings_, this->GetTableMetadata(left_table_index), this->GetTableMetadata(right_table_index),
		    left_table_index, right_table_index);
		all_nodes_.insert(static_cast<Node *>(join));
		make_edge(static_cast<Node *>(in_node_left), static_cast<Node *>(join));
		make_edge(static_cast<Node *>(in_node_right), static_cast<Node *>(join));

		return join;
	}

	template <typename T>
	struct function_traits : function_traits<decltype(&T::operator())> {};

	template <typename ClassType, typename ReturnType, typename... Args>
	struct function_traits<ReturnType (ClassType::*)(Args...) const> {
		using return_type = ReturnType;
		using arguments_tuple = std::tuple<Args...>;
	};

	// statefull agregation node
	template <typename F_aggr, typename F_getmatch, typename N>
	auto AggregateBy(F_getmatch get_match, F_aggr aggr_fun, N *in_node) -> TypedNode<std::remove_const_t<
	    std::remove_reference_t<std::tuple_element_t<2, typename function_traits<F_aggr>::arguments_tuple>>>> * {
		index table_index = this->maybe_create_table();

		using InType = typename N::value_type;
		using AggrArgs = typename function_traits<F_aggr>::arguments_tuple;

		using ThirdArg = std::tuple_element_t<2, AggrArgs>;
		using OutType = std::remove_const_t<std::remove_reference_t<ThirdArg>>;

		using MatchType = std::invoke_result_t<F_getmatch, const InType &>;

		TypedNode<OutType> *aggr = new AggregateByNode<InType, MatchType, OutType>(
		    in_node, aggr_fun, get_match, this, this->bp_, this->gb_settings_, this->GetTableMetadata(table_index),
		    table_index);
		this->all_nodes_.insert(static_cast<Node *>(aggr));
		this->make_edge(static_cast<Node *>(in_node), static_cast<Node *>(aggr));

		return aggr;
	}

	// Node processing

	/**
	 * @brief set next_node to next node to be processed
	 * @return all nodes from this graph that can be processed
	 */
	std::vector<Node *> GetNext() {
		std::scoped_lock(graph_latch_);
		if (AllProcesedPreviousLevel()) {
			// ok all nodes processed at last level we can restart
			if (current_level_ == this->levels_.size()) {
				this->FinishProduceRound();
			}

			const auto &ret = this->levels_[current_level_];
			this->current_level_++;
			return ret;
		} else {
			// produce queue is empty but current level isn't finished yet, no work to be done on this graph
			return {};
		}
	}

	// sets node state
	void Produced(Node *node, bool produced) {
		std::scoped_lock(graph_latch_);
		// set correct state to current node
		this->nodes_state_[node] = {NodeState::PROCESSED, produced};
	}

	/**
	 * @brief
	 * Created when all nodes are appended to graph, and processing can be started,
	 * this is called by WorkerPool->Start(Graph *g)
	 */
	void Start() {
		std::scoped_lock(graph_latch_);
		// after calling process once graph no longer will accept adding new nodes
		this->is_graph_running_ = true;
		if (this->topo_graph_.empty()) {
			this->topo_sort();
		}

		// this inits state
		FinishProduceRound();
	}

private:
	/**
	 * @return true if all nodes were processed in current iteration
	 */
	bool AllProcesedPreviousLevel() {
		// all nodes are processed when all sink nodes are processed
		if (this->current_level_ == 0) {
			return true;
		}

		for (const auto &node : this->levels_[this->current_level_ - 1]) {
			if (this->nodes_state_[node].first != NodeState::PROCESSED) {
				return false;
			}
		}

		return true;
	}

	// resets procesing state of graph, after this method
	bool FinishProduceRound() {
		bool produced = false;
		for (const auto &node : this->all_nodes_) {
			produced = produced || this->nodes_state_[node].second;
			this->nodes_state_[node] = {NodeState::NOT_PROCESSED, false};
		}

		this->current_level_ = 0;

		return produced;
	}

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

				//  node will only be dependent on its parents
				this->node_dependencies_[child].insert(node);
			}
		}

		// Organize nodes into levels
		this->levels_.resize(max_level + 1);
		for (const auto &[node, level] : node_levels) {
			this->levels_[level].push_back(node);
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

		if (in_edges_.contains(out_node)) {
			in_edges_[out_node].emplace_front(in_node);
		} else {
			in_edges_[out_node] = {in_node};
		}

		// this is not used at graph level but rather internally by node for ts updating
		in_node->AddOutNode(out_node);
	}

	void check_running() {
		if (this->is_graph_running_) {
			throw std::runtime_error("Graph is already running, can't add new nodes to it\n");
		}
	}

	/** @brief if this table state wasn't stored in metadatafile, create new table metadata */
	index maybe_create_table() {
		index table_index = this->next_table_index_;
		this->next_table_index_++;
		if (!this->tables_metadata_.contains(table_index)) {
			this->tables_metadata_[table_index] = MetaState {
			    {}, {},         {}, {}, this->graph_directory_ / ("delta_log_" + std::to_string(table_index) + ".bin"),
			    0,  table_index};
		}
		return table_index;
	}

	std::unordered_map<Node *, std::list<Node *>> out_edges_;
	std::unordered_map<Node *, std::list<Node *>> in_edges_;

	std::unordered_set<Node *> all_nodes_;

	std::set<Node *> sinks_;

	std::set<Node *> sources_;

	// List of nodes representing topological orders
	std::list<Node *> topo_graph_;
	// topolist but leveled, where node at level n can only depends on nodes on levels < n
	std::vector<std::vector<Node *>> levels_;
	// map of all dependencies for given node (in edges)
	std::unordered_map<Node *, std::set<Node *>> node_dependencies_;

	std::unordered_map<Node *, std::pair<NodeState, bool>> nodes_state_;

	std::mutex graph_latch_;

	bool is_graph_running_ = false;

	int current_level_ = 0;

	// for persisten storage, each node might store from 0 to 2 tables
	std::unordered_map<index, MetaState> tables_metadata_;
	int next_table_index_ = 0;

	std::filesystem::path graph_directory_;

	BufferPool *bp_;

	GarbageCollectSettings &gb_settings_;
};

} // namespace AliceDB

#endif
