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
   void Source(std::function<void(Type **out_data)>produce){
        Node *source_node =  new SourceNode(produce);
        nodes_.push_back(source_node);
   }


    template <typename Type>
    void Filter(Node *in_node, std::function<bool(const Type &) > condition){
        Node *filter_node =  new FilterNode<Type>(in_node, condition);
        nodes_.push_back(source_node);
    }

    template <typename InType, typename OutType>
	void Projection(Node *in_node,  std::function<OutType(const InType&)>projection){
        Node *projection =  new ProjectionNode<InType, OutType>(in_node, projection);
        nodes_.push_back(source_node);
    }


    template <typename Type>
	void Union(Node *in_node_left, Node *in_node_right){
        Node *union =  new UnionNode<Type>(in_node_left, in_node_right);
        nodes_.push_back(union);
    }
	
    template <typename Type>
	void Intersect(Node *in_node_left, Node *in_node_right){
        Node *intersect =  new IntersectNode<Type>(in_node_left, in_node_right);
        nodes_.push_back(intersect);
    }
	
    template <typename Type>
	void Except(Node *in_node_left, Node *in_node_right){
        Node *except =  new ExceptNode<Type>(in_node_left, in_node_right);
        nodes_.push_back(except);
    }


    template <typename InTypeLeft, typename InTypeRight, typename OutType>
	void CrossJoin(Node *in_node_left, Node *in_node_right, std::function<OutType(InTypeLeft,InTypeRight)>join_layout){
        Node *cross_join = new CrossJoinNode<InTypeLeft, InTypeRight, OutType>(in_node_left, in_node_right, join_layout);
        nodes_.push_back(cross_join);
    }

    template <typename InTypeLeft, typename InTypeRight, typename MatchType , typename OutType>
	void Join(Node *in_node_left, Node *in_node_right, 
		std::function<MatchType(InTypeLeft *)>get_match_left,
		std::function<MatchType(InTypeRight*)>get_match_right,
		std::function<OutType(InTypeLeft*, InTypeRight *)>join_layout){
            Node *join = new JoinNode<InTypeLeft, InTypeRight, MatchType, OutType>(in_node_left, in_node_right, get_match_left, get_match_right, join_layout);
            nodes_.push_back(join);
        }
	
    template <typename InType, Arithmetic OutType>
    void Sum(Node *in_node){
        Node *sum = new SumNode<InType, OutType>(in_node);
        nodes_.push_back(sum);
    }   

    template <typename InType, Arithmetic OutType>
    void Max(Node *in_node){
        Node *max = new MaxNode<InType, OutType>(in_node);
        nodes_.push_back(sum);
    }   




private:
    // vector of all created nodes
    std::vector<Node*> nodes_;

    // store topological order


};


} // namespace AliceDB


#endif