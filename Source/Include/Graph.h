#ifndef ALICEDBGRAPH
#define ALICEDBGRAPH

#include "Common.h"
#include "Node.h"

#include <stack>
#include <stdexcept>
#include <unordered_set>
#include <vector>

namespace AliceDB {
// ok now it should work with static non overlaping graphs

// after it will we can add mechanism to merge graphs dynamically (allow single
// source for multiple nodes, and also sinks to act as sources)

// change storage layer

// add sql layer

class Graph {
public:
  // creates graph instance, later it will also store and load from file info
  // about nodes for now we don't store persistent state
  // Graph() {}

  template <typename Type>
  Node *Source(Producer<Type> *prod, timestamp frontier_ts,
               int duration_us = 500) {
    Node *source_node = new SourceNode<Type>(prod, frontier_ts, duration_us);
    this->all_nodes_.insert(source_node);
    this->sources_.insert(source_node);
    return source_node;
  }

  /**
   * @brief creates sink node  & topological order of nodes
   */
  template <typename InType> Node *View(Node *in_node) {
    Node *sink = new SinkNode<InType>(in_node);
    this->make_edge(in_node, sink);
    this->all_nodes_.insert(sink);
    this->sinks_.insert(sink);

    // get list of all nodes creating this subgraph
    std::set<Node *> current_graph;
    std::stack<Node *> next_to_process;
    next_to_process.emplace(sink);
    while (!next_to_process.empty()) {
      Node *current = next_to_process.top();
      next_to_process.pop();
      current_graph.emplace(current);
      for (Node *in : current->Inputs()) {
        // well one node might be input to two different node, and we don't want
        // to process it two times also this will prevent infinite loop in case
        // of cycle but won't detect it
        if (!current_graph.contains(in)) {
          next_to_process.emplace(in);
        }
      }
    }

    /** @todo we should merge graph that share common node actually */
    /*
    // finally verify that there is no such node in this subgraph that is
    neither sink nor source and belong to other subgraph for(auto it =
    current_graph.begin(); it!= current_graph.end(); it++){
        if(this->all_nodes_[*it] == 1 && !this->sinks_.contains(*it) &&
    !this->sources_.contains(*it)){ throw std::runtime_error("Only sources and
    sinks cna belong to more than one graph");
        }
    }
    */

    // find all source Nodes that leads to this sink and create toposort
    this->topo_sort(current_graph);

    // return sink which will already be producing resoults
    return sink;
  }

  template <typename Type>
  Node *Filter(std::function<bool(const Type &)> condition, Node *in_node) {
    Node *filter = new FilterNode<Type>(in_node, condition);
    this->all_nodes_.insert(filter);
    this->make_edge(in_node, filter);
    return filter;
  }

  template <typename InType, typename OutType>
  Node *
  Projection(std::function<void(InType *, OutType *)> projection_function, Node *in_node) {
    Node *projection =
        new ProjectionNode<InType, OutType>(in_node, projection_function);
    this->all_nodes_.insert(projection);
    this->make_edge(in_node, projection);
    return projection;
  }

  template <typename Type> Node *Distinct(Node *in_node) {
    Node *distinct = new DistinctNode<Type>(in_node);
    this->all_nodes_.insert(distinct);
    this->make_edge(in_node, distinct);
    return distinct;
  }

  template <typename Type>
  Node *Union(Node *in_node_left, Node *in_node_right) {
    Node *plus = new PlusNode<Type>(in_node_left, in_node_right, false);
    this->all_nodes_.insert(plus);
    this->make_edge(in_node_left, plus);
    this->make_edge(in_node_right, plus);
    return this->Distinct<Type>(plus);
  }

  template <typename Type>
  Node *Except(Node *in_node_left, Node *in_node_right) {
    Node *plus = new PlusNode<Type>(in_node_left, in_node_right, true);
    this->all_nodes_.insert(plus);
    this->make_edge(in_node_left, plus);
    this->make_edge(in_node_right, plus);
    return this->Distinct<Type>(plus);
  }

  template <typename Type>
  Node *Intersect(Node *in_node_left, Node *in_node_right) {
    Node *intersect = new IntersectNode<Type>(in_node_left, in_node_right);
    this->all_nodes_.insert(intersect);
    this->make_edge(in_node_left, intersect);
    this->make_edge(in_node_right, intersect);
    return this->Distinct<Type>(intersect);
  }

  template <typename InTypeLeft, typename InTypeRight, typename OutType>
  Node *CrossJoin(std::function<OutType(InTypeLeft, InTypeRight)> join_layout,
                  Node *in_node_left, Node *in_node_right
                  ) {
    Node *cross_join = new CrossJoinNode<InTypeLeft, InTypeRight, OutType>(
        in_node_left, in_node_right, join_layout);
    this->all_nodes_.insert(cross_join);
    this->make_edge(in_node_left, cross_join);
    this->make_edge(in_node_right, cross_join);
    return cross_join;
  }

  template <typename InTypeLeft, typename InTypeRight, typename MatchType,
            typename OutType>
  Node *Join(std::function<MatchType(InTypeLeft *)> get_match_left,
             std::function<MatchType(InTypeRight *)> get_match_right,
             std::function<OutType(InTypeLeft *, InTypeRight *)> join_layout,
             Node *in_node_left, Node *in_node_right
             ) {
    Node *join = new JoinNode<InTypeLeft, InTypeRight, MatchType, OutType>(
        in_node_left, in_node_right, get_match_left, get_match_right,
        join_layout);
    this->all_nodes_.insert(join);
    this->make_edge(in_node_left, join);
    this->make_edge(in_node_right, join);
    return join;
  }

  template <typename InType, typename MatchType, typename OutType>
  Node *AggregateBy(std::function<void(OutType *, InType *, int)> aggr_fun,
                    std::function<void(InType *, MatchType *)> get_match, Node *in_node) {

    Node *aggr = new AggregateByNode<InType, MatchType, OutType>(
        in_node, aggr_fun, get_match);
    this->all_nodes_.insert(aggr);
    this->make_edge(in_node, aggr);
    return aggr;
  }

  /*
  loop that processes all nodes in topological order for <iters> times
  */
  void Process(int iters = 1) {
    for (int i = 0; i < iters; i++) {
      for (auto &graph : topo_graphs_) {
        for (Node *n : graph) {
          n->Compute();
        }
      }
    }
  }

private:
  void topo_sort(std::set<Node *> graph) {
    std::set<Node *> visited;
    std::stack<Node *> stack;

    // for each node:
    for (auto it = graph.begin(); it != graph.end(); it++) {
      Node *current = *it;

      if (visited.contains(current)) {
        continue;
      }

      std::set<Node *> current_run = {};

      // if visit return's 1 there was a cycle
      if (visit(current, visited, stack, current_run)) {
        throw std::runtime_error("[Error] graph contains cycle");
      }
    }

    // save this topo_graph as list
    std::list<Node *> topo_order;
    while (!stack.empty()) {
      topo_order.push_back(stack.top());
      stack.pop();
    }

    // mark all nodes as belonging to graph
    // for(auto it = graph.begin(); it != graph.end(); it++){
    //    this->all_nodes_[*it] = true;
    //}

    topo_graphs_.emplace(topo_order);


  }
  bool visit(Node *current, std::set<Node *> &visited,
             std::stack<Node *> &stack, std::set<Node *> &current_run) {
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

  std::unordered_map<Node *, std::list<Node *>> out_edges_;

  // all nodes, being marked means it allready belongs to some subgraph
  // std::unordered_map<Node*, bool> all_nodes_;
  // std::unordered_map<Node*, bool> all_nodes_;
  std::unordered_set<Node *> all_nodes_;

  // two kinds of nodes that are allowed to be part of more than one graph
  std::set<Node *> sinks_;
  std::set<Node *> sources_;

  // set of lists of nodes representing topological orders
  std::set<std::list<Node *>> topo_graphs_;
};

} // namespace AliceDB

#endif
