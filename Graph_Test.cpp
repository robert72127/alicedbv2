#include "gtest/gtest.h"

#include <filesystem>
#include <thread>
#include <vector>
#include <filesystem>
#include <string>
#include <functional>
#include <unordered_map>
#include <cstddef> // for std::size_t

struct Person{
        char Name[50];
        char LastName[50];
        int age;
        char FavoriteDog[50];
        float Money;
};

struct Dog{
        char Race[50];
        int MaxAge;
        float Height;
        float Weight;
        float Cost;
};

struct DogPrice{
    float TotalCost;
};

struct DogPriceHeight{
    float Height;
    float TotalCost;
};



// join on cost and race
struct HumanDogPairJoin{
        char Name[50];
        char LastName[50];
        int age;
        char FavoriteDog[50];
        float Money;
        int MaxAge;
        float Height;
        float Weight;
};       

struct HumanDogPairCartesian{
        char Name[50];
        char LastName[50];
        int age;
        char FavoriteDog[50];
        float Money;
        char Race[50];
        int MaxAge;
        float Height;
        float Weight;
        float Cost;
};       

struct Out{
    char Name[50];
    char LastName[50];
    char FavoriteDog[50];
};


template<typename T> 
void Filter(T *data_in, int in_size, T *data_out, std::function<bool(T)>filter ) {
   for(int i = 0; i < in_size/sizeof(T); i++){
        if(filter(data_in[i])){
            data_out = data_in[i];
            data_out += sizeof(T);
        }
   } 
}


template<typename T> 
void Union(T *data_in_1, T* data_in_2, int in_size_1, int in_size_2, T *data_out) {
    for(int i = 0; i < in_size_1/sizeof(T); i++){
            data_out = data_in_1[i];
            data_out += sizeof(T);
    } 
    for(int i = 0; i < in_size_2/sizeof(T); i++){
            data_out = data_in_2[i];
            data_out += sizeof(T);
    } 
}

template<typename T> 
void Difference(T *data_in_1, T* data_in_2, int in_size_1, int in_size_2, T *data_out) {
    for(int i = 0; i < in_size_1/sizeof(T); i++){
        break_outer = false;
        for(int j = 0; j < in_size_2/sizeof(T); j++){
            if(data_in_1[i] == data_in_2[j]){
                break_outer = true;
                break;
            }         
        }
        if(break_outer == false){

            data_out = data_in_1[i];
            data_out += sizeof(T);
        } 
                    
    } 
   
}

template<typename T> 
void Intersection(T *data_in_1, T* data_in_2, int in_size_1, int in_size_2, T *data_out) {
    for(int i = 0; i < in_size_1/sizeof(T); i++){
        break_outer = false;
        for(int j = 0; j < in_size_2/sizeof(T); j++){
            if(data_in_1[i] == data_in_2[j]){
                break_outer = true;
                break;
            }         
        }
        if(break_outer == true){

            data_out = data_in_1[i];
            data_out += sizeof(T);
        } 
    }  
}

template<typename T, typename M> 
void Project(T *data_in, int in_size, M *data_out, std::function<M(T)>proj ) {
   for(int i = 0; i < in_size/sizeof(T); i++){
            data_out = proj(data_in[i]);
            data_out += sizeof(T);
   } 
}


template<typename T, typename M, typename TM> 
void CrossJoin(T *data_in_1, M* data_in_2, int in_size_1, int in_size_2, T *data_out) {
    static_assert(std::is_trivial_v<TP>, "TP must be trivial");
    static_assert(std::is_standard_layout_v<TP>, "TP must be standard layout");

    for(int i = 0; i < in_size_1/sizeof(T); i++){
        for(int j = 0; j < in_size_2/sizeof(T); j++){
            if(data_in_1[i] == data_in_2[j]){
                // Copy T into TP
                std::memcpy(&data_out, &data_in_1[i], sizeof(T));

                // Find where P should start within TP using the offset of P's first member.
                // Let's say 'c' is P's first member in TP.
                constexpr std::size_t p_offset = offsetof(TM, c); 
                std::memcpy(reinterpret_cast<char*>(&tp) + p_offset, &data_in_2, sizeof(M));
            }         
        }
    }
}


// Join
template<typename T, typename M, typename TM> 
void JoinOn(T *data_in_1, M* data_in_2, int in_size_1, int in_size_2, T *data_out, std::function<bool(T, M)>match_function ) {
    static_assert(std::is_trivial_v<TP>, "TP must be trivial");
    static_assert(std::is_standard_layout_v<TP>, "TP must be standard layout");

    for(int i = 0; i < in_size_1/sizeof(T); i++){
        for(int j = 0; j < in_size_2/sizeof(T); j++){
            if(match_function(data_in_1[i], data_in_2[j])){
                // Copy T into TP
                std::memcpy(&data_out, &data_in_1[i], sizeof(T));

                // Find where P should start within TP using the offset of P's first member.
                // Let's say 'c' is P's first member in TP.
                constexpr std::size_t p_offset = offsetof(TM, c); 
                std::memcpy(reinterpret_cast<char*>(&tp) + p_offset, &data_in_2, sizeof(M));
            }         
        }
    }
}

// on final program it will be like
// Node<OutType> Join(Node<T> *Source1, Node<M> *Source2, 
//          std::function<bool(T, M)>match_function,
//          std::function<OutType(FullJoinType)>project_function);

template<typename T, typename M, typename FullJoinType, typename OutType> 
void Join(T *data_in_1, M* data_in_2, int in_size_1, int in_size_2, FullJoinType *tmp, int tmp_size, OutType *data_out, int out_size, 
            std::function<bool(T, M)>match_function, std::function<OutType(FullJoinType)>project_function) {

        JoinOn<T,N, FullJoinType>(data_in_1,data_in_2,in_size_1,in_size_2,tmp); 
        Project<FullJoinType, OutType>(tmp,tmp_size, data_out, out_size,project_function);
}
// endjoin


// aggregate  1) group by specific field, 2) for all matched compute result using agg_function

// A generic aggregation operator
// T: Type of the input elements
// R: Type of the result (aggregate)
template<typename T, typename R>
R Aggregate(T *data_in, int in_size, std::function<R(R, T)> agg_func, R initial_value) {
    // Compute the number of elements
    int count = in_size / sizeof(T);
    R result = initial_value;
    for(int i = 0; i < count; i++){
        result = agg_func(result, data_in[i]);
    }
    return result;
}


// GroupByAggregate
// T: Type of input records
// K: Type of the key returned by key_func (the grouping attribute's type)
// R: Type of the aggregation result
//
// Parameters:
// data_in: Pointer to input data array of type T
// in_size: Size in bytes of the input data
// key_func: A function T -> K that extracts a grouping key from a record.
// agg_func: A function (R, T) -> R that aggregates a new record into the current aggregate.
// initial_value: The initial aggregate value for each new key.
//
// Returns:
// An associative container mapping from keys (K) to aggregated results (R).

template<typename T, typename K, typename R>
auto GroupByAggregate(
    T *data_in, 
    int in_size,
    std::function<K(const T&)> key_func,
    std::function<R(R, const T&)> agg_func,
    R initial_value
) -> std::unordered_map<K, R>
{
    int count = in_size / sizeof(T);
    std::unordered_map<K, R> groups;

    for (int i = 0; i < count; i++){
        const T& element = data_in[i];
        K key = key_func(element);

        // If the key doesn't exist yet, insert it with the initial value
        auto result = groups.try_emplace(key, initial_value);

        // Aggregate this element into the group's current aggregate value
        R& current_aggregate = result.first->second;
        current_aggregate = agg_func(current_aggregate, element);
    }

    return groups;
}




TEST(TableTest, TableCreationTest) {
}

class Graph;

TEST(PROGRAMTest, FakeTest){
    auto *humans = new Person[50];
    auto *dogs1 = new Dog[40];
    auto *dogs2 = new Dog[40];
    
    auto *tmp = new HumanDogPairCartesian[50];
    auto *final = new HumanDogPairJoin[50];


    auto *prices = Aggregate<Dog, DogPrice>(dogs, 50, total_cost, 1, (Dog d, int acc){ return acc + d->price })

    auto *prices = AggregateBy<Dog, DogPrice>(dogs, 50, total_cost, 1, 
        (Dog a)-> DogPriceHeight { return {a->price, a->height};},
        (DogPrice d, int acc){ return {acc + d->price, a->height}; });

    auto *prices = AggregateBy<Dog, DogPrice>(dogs, 50, total_cost, 1, 
        (Dog a)-> DogPriceHeight { return {a->price, a->height}; },
        (DogPriceHeight d, int acc) { return {acc + d->price, a->height}; });



    /*
    auto *human_dog_pairs = Join<Person, Dog, HumanDogPairCartesian, HumanDogPairJoin>(humans, 50, dogs1, 50, tmp, 50, final, 50,
     (Person p, Dog g)-> bool {return (p->FavoriteDog == g.Race && p->Money == d->Cost);}, 
     (HumanDogPairCartesian pg) -> HumanDogPairJoin{ return {pg->Name, pg->LastName, pg->age, pg->FavoriteDog, pg->Money, pg->MaxAge, pg->Height, pg->Weight};})
    */
    /*
    auto *g = new Graph();


    // query list those human_name, surname dog_name
    // for those humans who can buy their favourite unexpensive dog

    auto *humans = g.source<Person>([]{
        while(true){
            // read data from file
            struct *p = new Person{"Jan", "Kowalski",15, "Husky", 5500.5};
        }
    })

    auto *dogs = g.source<Dog>([]{

        while(true){
            // read data from file
            // race max age, weight, height
            struct *p = new Dog{"Hasky",9,33, 55, 3200};
        }
    })

    // filter
    auto *adults = g.filter<Person>(humans, (Person *p){
        return p->age > 18;
    });

    // filter
    auto *cheap_dogs = g.filter<Dog>(dogs, (Dog d*) -> bool {
        return d->Cost > 1500;
    });


    // select
    struct CostOfRace{
        std::string Race;
        float cost
    };

    auto *cost_to_race = g.select<Dog, CostofRace>(dogs, (Dog d*) ->CostOfRace {
        return {d->Race, d->Price};
    });

    //cartesian product

    auto *prod = g.product<Dog, Human, DogHuman>(dogs, humans (Dog d*, Human *h) -> DogHuman{
        //return skeljenie dancych scastowane na nowy typ

    });


    // intersection
    auto *inter = g.Intersert<Dog>(dogs1, dogs2);
    //difference
    auto *inter = g.Difference<Dog>(dogs1, dogs2);
    //intersection
    auto *inter = g.Interset<Dog>(dogs1, dogs2);



    // join
    auto *human_dog_pairs = g.join<Person, Dog, HumanDogPair>(humans, dogs (char *left, char *right, char *out){
        auto *d = (Dog*)left;
        auto *h = (Human*)right;
        auto *hp = (HumanDogPair*)(out);
        
        hp->Name = h->Name;
        hp->LastName = h->name;
        hp->age = h->age;
        hp->FavoriteDog = h->FavoriteDog;
        hp->Money = h->Money;
        hp->MaxAge = d->MaxAge;
        hp->Height = d->Height;
        hp->Weight = d->Weight;
        hp->Cost = h->Cost;
    })


    auto final_filter = g.filter<HumanDogPair>((char *data) -> bool {
        auto *hp = (HumanDogPair*)(out);
        return (hp->Cost <= hp->Money);
    });

    auto final_cast = g.Projection<HumanDogPair, Out>((char * data, char *out_data){
        auto *hp = (HumanDogPair*)(data);
        auto *out = (Out*)(out_data);
        
        out->Name = hp->Name;
        out->LastName = hp->LastName;
        out->FavoriteDog = hp->FavoriteDog; 
    })
    */
}