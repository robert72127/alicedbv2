#include <iostream>
#include <fstream>
#include <filesystem>
#include <thread>
#include <chrono>

#include "gtest/gtest.h"

#include "AliceDB.h"

/*
// Seed the random number generator
TEST(MULTINODE_TEST, multinode_test){
    std::string dogs_fname = "dogs.txt";
    std::string people_fname = "people.txt";
    prepare_people_data_file(people_fname);
    prepare_dog_data_file(dogs_fname);

    std::filesystem::path db_path = "./database";
    unsigned int worker_threads_cnt = 2;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    // define new graph instance
    auto *g = db->CreateGraph();

    // define processing graph
    auto *view = 
        g->View(
            g->Except(
                g->Source(prod_people_2,0),
                g->Filter(
                    [](const Person &p) -> bool {return p.age > 18;},
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
                )
            )
        );
    auto *view = 
        g->View(
            g->Projection(
                [](const JoinDogPerson &p) {
                    return CanAffordDog{
                       .name=p.name,
                       .surname=p.surname, 
                    };
                },
                g->Filter(
                    [](const JoinDogPerson &p) -> bool {return p.account_balace > p.dog_cost;},
                    g->Join(
                        [](const Person &p)  { return p.favourite_dog_race;},
                        [](const Dog &d)  { return d.name;},
                        [](const Person &p, const Dog &d) { 
                            return  JoinDogPerson{
                                .name=p.name,
                                .surname=p.surname,
                                .favourite_dog_race=d.name,
                                .dog_cost=d.cost,
                                .account_balace=p.account_balance,
                                .age=p.age
                            };
                        },
                        g->Filter(
                            [](const Person &p) -> bool {return p.age > 18;},
                            g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
                        ),
                        g->Source<Dog>(AliceDB::ProducerType::FILE, dogs_fname, parseDog,0)
                    )
                    
                )
            )
        );
    auto *view = g->View(
                    g->Join(
                        [](const Person &p)  { return p.favourite_dog_race;},
                        [](const Dog &d)  { return d.name;},
                        [](const Person &p, const Dog &d) { 
                            return  JoinDogPerson{
                                .name=p.name,
                                .surname=p.surname,
                                .favourite_dog_race=d.name,
                                .dog_cost=d.cost,
                                .account_balace=p.account_balance,
                                .age=p.age
                            };
                        },
                        g->Filter(
                            [](const Person &p) -> bool {return p.age > 18;},
                            g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
                        ),
                        g->Source<Dog>(AliceDB::ProducerType::FILE, dogs_fname, parseDog,0)
                    )
                    
                );

    // start processing data
    db->StartGraph(g);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    db->StopGraph(g);

    //AliceDB::SinkNode<CanAffordDog> *real_sink = reinterpret_cast<AliceDB::SinkNode<CanAffordDog>*>(view);
    //AliceDB::SinkNode<Person> *real_sink = reinterpret_cast<AliceDB::SinkNode<Person>*>(view);
    AliceDB::SinkNode<JoinDogPerson> *real_sink = reinterpret_cast<AliceDB::SinkNode<JoinDogPerson>*>(view);

    //real_sink->Print(AliceDB::get_current_timestamp()*2, print_canafforddog);
    //real_sink->Print(AliceDB::get_current_timestamp()*2, print_person);
    real_sink->Print(AliceDB::get_current_timestamp()*2, print_joindogperson);

    db->Shutdown();

}
*/