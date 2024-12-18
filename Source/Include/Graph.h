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
  TypedNode<Type> *Source(Producer<Type> *prod, timestamp frontier_ts,
               int duration_us = 500) {
    TypedNode<Type> *source_node = new SourceNode<Type>(prod, frontier_ts, duration_us);
    this->all_nodes_.insert(static_cast<Node*>(source_node));
    this->sources_.insert(static_cast<Node*>(source_node));
    return source_node;
  }

  /**
   * @brief creates sink node  & topological order of nodes
   */
  template <typename InType>
  TypedNode<InType> *View(TypedNode<InType> *in_node) {
    TypedNode<InType> *sink = new SinkNode<InType>(in_node);
    this->make_edge(static_cast<Node*>(in_node), static_cast<Node*>(sink));
    this->all_nodes_.insert(static_cast<Node*>(sink));
    this->sinks_.insert(static_cast<Node*>(sink));

    // return sink which will already be producing resoults
    return sink;
  }

  template <typename Type>
  TypedNode<Type> *Filter(std::function<bool(const Type &)> condition, TypedNode<Type> *in_node) {
    TypedNode<Type> *filter = new FilterNode<Type>(in_node, condition);
    this->all_nodes_.insert(static_cast<Node*>(filter));
    this->make_edge( static_cast<Node*>(in_node), static_cast<Node*>(filter));
    return filter;
  }

  template <typename InType, typename OutType>
  TypedNode<OutType> *
  Projection(std::function<OutType(const InType &)> projection_function, TypedNode<InType> *in_node) {
    TypedNode<OutType> *projection =
        new ProjectionNode<InType, OutType>(in_node, projection_function);
    this->all_nodes_.insert(static_cast<Node*>(projection));
    this->make_edge(static_cast<Node*>(in_node), static_cast<Node*>(projection));
    return projection;
  }

  template <typename Type> 
  TypedNode<Type> *Distinct(TypedNode<Type> *in_node) {
    TypedNode<Type> *distinct = new DistinctNode<Type>(in_node);
    this->all_nodes_.insert(static_cast<Node*>(distinct));
    this->make_edge(static_cast<Node*>(in_node), static_cast<Node*>(distinct));
    return distinct;
  }

  template <typename Type>
  TypedNode<Type> *Union(TypedNode<Type> *in_node_left, TypedNode<Type> *in_node_right) {
    TypedNode<Type> *plus = new PlusNode<Type>(in_node_left, in_node_right, false);
    this->all_nodes_.insert(static_cast<Node*>(plus));
    this->make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(plus));
    this->make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(plus));
    return this->Distinct<Type>(plus);
  }

  template <typename Type>
  TypedNode<Type> *Except(TypedNode<Type> *in_node_left, TypedNode<Type> *in_node_right) {
    TypedNode<Type> *plus = new PlusNode<Type>(in_node_left, in_node_right, true);
    this->all_nodes_.insert(static_cast<Node*>(plus));
    this->make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(plus));
    this->make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(plus));
    return this->Distinct<Type>(plus);
  }

  template <typename Type>
  TypedNode<Type> *Intersect(TypedNode<Type> *in_node_left, TypedNode<Type> *in_node_right) {
    TypedNode<Type> *intersect = new IntersectNode<Type>(in_node_left, in_node_right);
    this->all_nodes_.insert(static_cast<Node*>(intersect));
    this->make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(intersect));
    this->make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(intersect));
    return this->Distinct<Type>(intersect);
  }

  template <typename InTypeLeft, typename InTypeRight, typename OutType>
  TypedNode<OutType> *CrossJoin(std::function<OutType(const InTypeLeft&, const InTypeRight&)> join_layout,
                  TypedNode<InTypeLeft> *in_node_left, TypedNode<InTypeRight> *in_node_right
                  ) {
    TypedNode<OutType> *cross_join = new CrossJoinNode<InTypeLeft, InTypeRight, OutType>(
        in_node_left, in_node_right, join_layout);
    
    this->all_nodes_.insert(static_cast<Node*>(cross_join));
    this->make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(cross_join));
    this->make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(cross_join));
    return cross_join;
  }

  template <typename InTypeLeft, typename InTypeRight, typename MatchType,
            typename OutType>
  TypedNode<OutType> *Join(std::function<MatchType(const InTypeLeft &)> get_match_left,
             std::function<MatchType(const InTypeRight &)> get_match_right,
             std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout,
             TypedNode<InTypeLeft> *in_node_left, TypedNode<InTypeRight> *in_node_right
             ) {
    TypedNode<OutType> *join = new JoinNode<InTypeLeft, InTypeRight, MatchType, OutType>(
        in_node_left, in_node_right, get_match_left, get_match_right,
        join_layout);
    
    this->all_nodes_.insert(static_cast<Node*>(join));
    this->make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(join));
    this->make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(join));
    return join;
  }

  template <typename InType, typename MatchType, typename OutType>
  TypedNode<OutType> *AggregateBy(std::function<void(OutType *, InType *, int)> aggr_fun,
                    std::function<void(InType *, MatchType *)> get_match, TypedNode<InType> *in_node) {

    TypedNode<OutType> *aggr = new AggregateByNode<InType, MatchType, OutType>(in_node, aggr_fun, get_match);
    this->all_nodes_.insert(static_cast<Node*>(aggr));
    this->make_edge(static_cast<Node*>(in_node), static_cast<Node*>(aggr));
    return aggr;
  }

  /*
  loop that processes all nodes in topological order for <iters> times
  */
  void Process(int iters = 1) {
    if(this->topo_graph_.empty()){
        this->topo_sort();
    }
    for (int i = 0; i < iters; i++) {
        for (Node *n : topo_graph_) {
          n->Compute();
        }
      }
  }

private:
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

      // if visit return's 1 there was a cycle
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
  std::list<Node *> topo_graph_;
};

} // namespace AliceDB

#endif
