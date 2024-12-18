#ifndef ALICEDBGRAPH
#define ALICEDBGRAPH

#include <functional>
#include <list>
#include <set>
#include <stack>
#include <stdexcept>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "Common.h"
#include "Node.h"

namespace AliceDB {

class Graph {
 public:
  template <typename P>
  auto Source(P* prod, timestamp frontier_ts, int duration_us = 500)
      -> TypedNode<typename P::value_type>* {
    using Type = typename P::value_type;
    auto* source_node = new SourceNode<Type>(prod, frontier_ts, duration_us);
    all_nodes_.insert(static_cast<Node*>(source_node));
    sources_.insert(static_cast<Node*>(source_node));
    return source_node;
  }

  template <typename N>
  auto View(N* in_node) -> TypedNode<typename N::value_type>* {
    using InType = typename N::value_type;
    TypedNode<InType>* sink = new SinkNode<InType>(in_node);
    make_edge(static_cast<Node*>(in_node), static_cast<Node*>(sink));
    all_nodes_.insert(static_cast<Node*>(sink));
    sinks_.insert(static_cast<Node*>(sink));
    return sink;
  }

  template <typename F, typename N>
  auto Filter(F condition, N* in_node) -> TypedNode<typename N::value_type>* {
    using Type = typename N::value_type;
    auto* filter = new FilterNode<Type>(in_node, condition);
    all_nodes_.insert(static_cast<Node*>(filter));
    make_edge(static_cast<Node*>(in_node), static_cast<Node*>(filter));
    return filter;
  }

  template <typename F, typename N>
  auto Projection(F projection_function, N* in_node)
      -> TypedNode<std::invoke_result_t<F, const typename N::value_type&>>* {
    using InType = typename N::value_type;
    using OutType = std::invoke_result_t<F, const InType&>;
    TypedNode<OutType>* projection =
        new ProjectionNode<InType, OutType>(in_node, projection_function);
    all_nodes_.insert(static_cast<Node*>(projection));
    make_edge(static_cast<Node*>(in_node), static_cast<Node*>(projection));
    return projection;
  }

  template <typename N>
  auto Distinct(N* in_node) -> TypedNode<typename N::value_type>* {
    using Type = typename N::value_type;
    auto* distinct = new DistinctNode<Type>(in_node);
    all_nodes_.insert(static_cast<Node*>(distinct));
    make_edge(static_cast<Node*>(in_node), static_cast<Node*>(distinct));
    return distinct;
  }

  template <typename N, typename N2>
  auto Union(N* in_node_left, N* in_node_right) -> TypedNode<typename N::value_type>* {
    using Type = typename N::value_type;
    auto* plus = new PlusNode<Type>(in_node_left, in_node_right, false);
    all_nodes_.insert(static_cast<Node*>(plus));
    make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(plus));
    make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(plus));
    return Distinct(plus);
  }

  template <typename N>
  auto Except(N* in_node_left, N* in_node_right) -> TypedNode<typename N::value_type>* {
    using Type = typename N::value_type;
    TypedNode<Type>* plus = new PlusNode<Type>(in_node_left, in_node_right, true);
    all_nodes_.insert(static_cast<Node*>(plus));
    make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(plus));
    make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(plus));
    return Distinct(plus);
  }

  template <typename N>
  auto Intersect(N* in_node_left, N* in_node_right) -> TypedNode<typename N::value_type>* {
    using Type = typename N::value_type;
    TypedNode<Type>* intersect = new IntersectNode<Type>(in_node_left, in_node_right);
    all_nodes_.insert(static_cast<Node*>(intersect));
    make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(intersect));
    make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(intersect));
    return Distinct(intersect);
  }

  // crossjoin deduces outtype from join_layout and input node types
  template <typename F, typename NL, typename NR>
  auto CrossJoin(F join_layout, NL* in_node_left, NR* in_node_right)
      -> TypedNode<std::invoke_result_t<F, const typename NL::value_type&,
                                        const typename NR::value_type&>>* {
    using InTypeLeft = typename NL::value_type;
    using InTypeRight = typename NR::value_type;
    using OutType = std::invoke_result_t<F, const InTypeLeft&, const InTypeRight&>;
    TypedNode<OutType>* cross_join = new CrossJoinNode<InTypeLeft, InTypeRight, OutType>(
        in_node_left, in_node_right, join_layout);
    all_nodes_.insert(static_cast<Node*>(cross_join));
    make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(cross_join));
    make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(cross_join));
    return cross_join;
  }

  // join deduces matchType get_match_left, get_match_right, and mutType from join_layout
  template <typename F_left, typename F_right, typename F_join, typename NL, typename NR>
  auto Join(F_left get_match_left, F_right get_match_right, F_join join_layout,
            NL* in_node_left, NR* in_node_right)
      -> TypedNode<std::invoke_result_t<F_join, const typename NL::value_type&,
                                        const typename NR::value_type&>>* {
    using InTypeLeft = typename NL::value_type;
    using InTypeRight = typename NR::value_type;
    using MatchTypeLeft = std::invoke_result_t<F_left, const InTypeLeft&>;
    using MatchTypeRight = std::invoke_result_t<F_right, const InTypeRight&>;
    static_assert(std::is_same_v<MatchTypeLeft, MatchTypeRight>,
                  "Left/Right match keys differ");
    using MatchType = MatchTypeLeft;
    using OutType = std::invoke_result_t<F_join, const InTypeLeft&, const InTypeRight&>;
    TypedNode<OutType>* join = new JoinNode<InTypeLeft, InTypeRight, MatchType, OutType>(
        in_node_left, in_node_right, get_match_left, get_match_right, join_layout);
    all_nodes_.insert(static_cast<Node*>(join));
    make_edge(static_cast<Node*>(in_node_left), static_cast<Node*>(join));
    make_edge(static_cast<Node*>(in_node_right), static_cast<Node*>(join));
    return join;
  }

  /** @todo this guy needs some work done, maybe even different api  */
  template <typename InType, typename MatchType, typename OutType>
  TypedNode<OutType>* AggregateBy(std::function<void(OutType*, InType*, int)> aggr_fun,
                                  std::function<void(InType*, MatchType*)> get_match,
                                  TypedNode<InType>* in_node) {
    TypedNode<OutType>* aggr =
        new AggregateByNode<InType, MatchType, OutType>(in_node, aggr_fun, get_match);
    this->all_nodes_.insert(static_cast<Node*>(aggr));
    this->make_edge(static_cast<Node*>(in_node), static_cast<Node*>(aggr));
    return aggr;
  }

  /*
  loop that processes all nodes in topological order for <iters> times
  */
  void Process(int iters = 1) {
    if (this->topo_graph_.empty()) {
      this->topo_sort();
    }
    for (int i = 0; i < iters; i++) {
      for (Node* n : topo_graph_) {
        n->Compute();
      }
    }
  }

 private:
  void topo_sort() {
    std::set<Node*> visited;
    std::stack<Node*> stack;

    // for each node:
    for (auto it = all_nodes_.begin(); it != all_nodes_.end(); it++) {
      Node* current = *it;

      if (visited.contains(current)) {
        continue;
      }

      std::set<Node*> current_run = {};

      // if visit return's 1 there was a cycle
      if (visit(current, visited, stack, current_run)) {
        throw std::runtime_error("[Error] graph contains cycle");
      }
    }

    // save this topo_graph as list
    std::list<Node*> topo_order;
    while (!stack.empty()) {
      topo_graph_.push_back(stack.top());
      stack.pop();
    }
  }
  bool visit(Node* current, std::set<Node*>& visited, std::stack<Node*>& stack,
             std::set<Node*>& current_run) {
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
      for (Node* neighbour : out_edges_[current]) {
        has_cycle |= visit(neighbour, visited, stack, current_run);
      }
    }
    stack.emplace(current);
    return has_cycle;
  }

  void make_edge(Node* in_node, Node* out_node) {
    if (out_edges_.contains(in_node)) {
      out_edges_[in_node].emplace_front(out_node);
    } else {
      out_edges_[in_node] = {out_node};
    }
  }

  std::unordered_map<Node*, std::list<Node*>> out_edges_;

  std::unordered_set<Node*> all_nodes_;

  std::set<Node*> sinks_;
  std::set<Node*> sources_;

  // List of nodes representing topological orders
  std::list<Node*> topo_graph_;
};

}  // namespace AliceDB

#endif
