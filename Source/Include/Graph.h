#ifndef ALICEDBGRAPH
#define ALICEDBGRAPH

#include "Node.h"
#include "Common.h"

#include <vector>


namespace AliceDB
{

class Graph{
    // creates graph instance, later it will also store and load from file info about nodes
    // for now we don't store persistent state
    Graph() {}


   template <typename Type>
   Node *Source(std::function<void(Type **out_data)>produce){
        Node *source_node =  new SourceNode(produce);
        this->nodes_.push_back(source_node);
        return source_node;
   }


    template <typename Type>
    Node *Filter(Node *in_node, std::function<bool(const Type &) > condition){
        Node *filter =  new FilterNode<Type>(in_node, condition);
        this->nodes_.push_back(filter);
        this->create_edge(in_node, filter);
        return filter;
    }

    template <typename InType, typename OutType>
	Node *Projection(Node *in_node,  std::function<OutType(const InType&)>projection_function){
        Node *projection =  new ProjectionNode<InType, OutType>(in_node, projection_function);
        this->nodes_.push_back(projection);
        this->create_edge(in_node, projection);
        return projection;
    }


    template <typename Type>
	Node *Union(Node *in_node_left, Node *in_node_right){
        Node *_union =  new UnionNode<Type>(in_node_left, in_node_right);
        this->nodes_.push_back(_union);
        this->create_edge(in_node_left, _union);
        this->create_edge(in_node_right, _union);
        return _union;
    }
	
    template <typename Type>
	Node *Intersect(Node *in_node_left, Node *in_node_right){
        Node *intersect =  new IntersectNode<Type>(in_node_left, in_node_right);
        this->nodes_.push_back(intersect);
        this->create_edge(in_node_left, intersect);
        this->create_edge(in_node_right, intersect);
        return intersect;
    }
	
    template <typename Type>
	Node *Except(Node *in_node_left, Node *in_node_right){
        Node *except =  new ExceptNode<Type>(in_node_left, in_node_right);
        this->nodes_.push_back(except);
        this->create_edge(in_node_left, except);
        this->create_edge(in_node_right, except);
        return except;
    }


    template <typename InTypeLeft, typename InTypeRight, typename OutType>
	Node *CrossJoin(Node *in_node_left, Node *in_node_right, std::function<OutType(InTypeLeft,InTypeRight)>join_layout){
        Node *cross_join = new CrossJoinNode<InTypeLeft, InTypeRight, OutType>(in_node_left, in_node_right, join_layout);
        this->nodes_.push_back(cross_join);
        this->create_edge(in_node_left, cross_join);
        this->create_edge(in_node_right, cross_join);
        return cross_join;
    }

    template <typename InTypeLeft, typename InTypeRight, typename MatchType , typename OutType>
	Node *Join(Node *in_node_left, Node *in_node_right, 
		std::function<MatchType(InTypeLeft *)>get_match_left,
		std::function<MatchType(InTypeRight*)>get_match_right,
		std::function<OutType(InTypeLeft*, InTypeRight *)>join_layout){
            Node *join = new JoinNode<InTypeLeft, InTypeRight, MatchType, OutType>(in_node_left, in_node_right, get_match_left, get_match_right, join_layout);
            this->nodes_.push_back(join);
            this->create_edge(in_node_left, join);
            this->create_edge(in_node_right, join);
            return join;
        }
	
    template <typename InType, Arithmetic OutType>
    Node *Sum(Node *in_node){
        Node *sum = new SumNode<InType, OutType>(in_node);
        this->nodes_.push_back(sum);
        this->create_edge(in_node, sum);
        return sum;
    }   

    template <typename InType, Arithmetic OutType>
    Node *Max(Node *in_node){
        Node *max = new MaxNode<InType, OutType>(in_node);
        this->nodes_.push_back(max);
        this->create_edge(in_node, max);
        return max;
    }   



private:
    void create_edge(Node *in_node, Node *out_node){
        if(!out_nodes_.contains(in_node)){
            out_nodes_[in_node] = {out_node};
        } else{
            out_nodes_[in_node].push_back(out_node);
        }
    }

    // vector of all created nodes
    std::vector<Node*> nodes_;

    // maps list of out nodes for given Node
    std::map<Node*, std::list<Node*>> out_nodes_;

    // store topological order
};


} // namespace AliceDB


#endif