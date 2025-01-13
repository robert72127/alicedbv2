#include <iostream>
#include <fstream>
#include <filesystem>

#include "gtest/gtest.h"

#include "Node.h"
#include "Graph.h"
#include "Producer.h"
#include "Common.h"
#include "Tuple.h"
#include "WorkerPool.h"

struct Person {
    std::array<char,50> name;
    std::array<char,50> surname;
    int age;
};

struct Name{
    std::array<char,50> name;
};

bool filter_adult(const Person &p){
    return p.age > 18;
}

Name proj_names(const Person &p){
    return Name{.name=p.name};
}

bool parseLine(const std::string &line, AliceDB::Tuple<Person> *p) {

            std::istringstream iss(line);
            AliceDB::timestamp ts;
            std::string insert_delete;
            char name[50];
            char surname[50];
            int age;

            if (!(iss >> insert_delete >> ts >> name >> surname >> age)) {
                return false; // parse error
            }

            // Copy name to the char array field. Ensure no overflow:
            std::strncpy(p->data.name.data(), name, sizeof(p->data.name));
            std::strncpy(p->data.surname.data(), surname, sizeof(p->data.surname));
            p->data.age = age;
            p->delta.count = (insert_delete == "insert")? 1 : -1;
            p->delta.ts = ts;
            return true;
}
 
 void print_people(const char *data){
    const Person *p = reinterpret_cast<const Person*>(data);

    std::cout<<p->name.data() << " " << p->surname.data() << " " << p->age << std::endl; 
}


void print_name(const char *data){
    const Name *n = reinterpret_cast<const Name *>(data);

    std::cout<< n->name.data() <<  std::endl; 
} 


// generate bunch of people and write this data to some file, thanks chat gpt
std::array<std::string, 100> surnames = {
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
        "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
        "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
        "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
        "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes",
        "Stewart", "Morris", "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper",
        "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
        "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
        "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long", "Ross", "Foster", "Jimenez"
};

std::array<std::string, 100> names = {
        "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
        "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
        "Christopher", "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
        "Donald", "Ashley", "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
        "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa", "Edward", "Deborah",
        "Ronald", "Stephanie", "Timothy", "Rebecca", "Jason", "Sharon", "Jeffrey", "Laura", "Ryan", "Cynthia",
        "Jacob", "Kathleen", "Gary", "Amy", "Nicholas", "Shirley", "Eric", "Angela", "Jonathan", "Helen",
        "Stephen", "Anna", "Larry", "Brenda", "Justin", "Pamela", "Scott", "Emma", "Brandon", "Nicole",
        "Frank", "Samantha", "Benjamin", "Katherine", "Gregory", "Christine", "Samuel", "Debra", "Raymond", "Rachel",
        "Patrick", "Catherine", "Alexander", "Carolyn", "Jack", "Janet", "Dennis", "Ruth", "Jerry", "Maria"
};
/*
TEST(STATELESS_TEST, single_version_test){

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it
    std::string file_name = "./people.txt";

    std::ofstream file_writer{file_name};


    for (auto &name : names){
            for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp()) + " "  + name + " " + surname + " " +  std::to_string(age);
                //std::cout << test_str <<std::endl;
                file_writer << person_str << std::endl;
            }
    }

    file_writer.close();

    AliceDB::Producer<Person> *prod = new AliceDB::FileProducer<Person>(file_name,parseLine);


    //AliceDB::Tuple<Person> *tpl = new AliceDB::Tuple<Person>;

    AliceDB::Node *source = new AliceDB::SourceNode<Person>(prod, 5);
    
    AliceDB::Queue *source_queue = source->Output();
    
    AliceDB::Node *adults = new AliceDB::FilterNode<Person>(source, filter_adult);
    
    AliceDB::Queue *adults_queue = adults->Output();

    AliceDB::Node *name_getter = new AliceDB::ProjectionNode<Person, Name>(adults, proj_names);
    
    AliceDB::Queue *name_queue = name_getter->Output();

    AliceDB::Node *sink = new AliceDB::SinkNode<Name>(name_getter);

    for(int i = 0; i < 10; i++){
        source->Compute();
        adults->Compute();
        name_getter->Compute();
        sink->Compute();
    }

    AliceDB::SinkNode<Name> *real_sink = reinterpret_cast<AliceDB::SinkNode<Name>*>(sink);

    real_sink->Print(AliceDB::get_current_timestamp(), print_name );

    // check queue contents, make sure they are empty
    int count = 0;
    const char *data;
    while(source_queue->GetNext(&data)){count++;}
    while(adults_queue->GetNext(&data)){count++;}
    while(name_queue->GetNext(&data)){count++;}
    ASSERT_EQ(count, 0);


    // delete file
    std::filesystem::remove("./people.txt");
}


TEST(STATELESS_TEST, test_late_data){

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it
    std::string file_name = "./people.txt";

    std::ofstream file_writer{file_name};


    for (auto &name : names){
            for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                std::string person_str = "insert " + std::to_string(2*AliceDB::get_current_timestamp()) + " "  + name + " " + surname + " " +  std::to_string(age);
                //std::cout << test_str <<std::endl;
                file_writer << person_str << std::endl;
            }
    }

    file_writer.close();

    AliceDB::Producer<Person> *prod = new AliceDB::FileProducer<Person>(file_name,parseLine);


    //AliceDB::Tuple<Person> *tpl = new AliceDB::Tuple<Person>;

    AliceDB::Node *source = new AliceDB::SourceNode<Person>(prod, 5);
    
    AliceDB::Queue *source_queue = source->Output();
    
    AliceDB::Node *adults = new AliceDB::FilterNode<Person>(source, filter_adult);
    
    AliceDB::Queue *adults_queue = adults->Output();

    AliceDB::Node *name_getter = new AliceDB::ProjectionNode<Person, Name>(adults, proj_names);
    
    AliceDB::Queue *name_queue = name_getter->Output();

    AliceDB::Node *sink = new AliceDB::SinkNode<Name>(name_getter);

    for(int i = 0; i < 10; i++){
        source->Compute();
        adults->Compute();
        name_getter->Compute();
        sink->Compute();
    }

    AliceDB::SinkNode<Name> *real_sink = reinterpret_cast<AliceDB::SinkNode<Name>*>(sink);

    real_sink->Print(AliceDB::get_current_timestamp(), print_name);

    // check queue contents, make sure they are empty
    int count = 0;
    const char *data;
    while(source_queue->GetNext(&data)){count++;}
    while(adults_queue->GetNext(&data)){count++;}
    while(name_queue->GetNext(&data)){count++;}
    ASSERT_EQ(count, 0);


    // delete file
    std::filesystem::remove("./people.txt");
}

TEST(STATELESS_TEST, test_insert_delete_data){

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it
    std::string file_name = "./people.txt";

    std::ofstream file_writer{file_name};
    for (auto &name : names){
                for(auto &surname: surnames ){
                    int age = std::rand() % 101; // Random number between 0 and 100
                    std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp()) + " "  + name + " " + surname + " " +  std::to_string(age);
                    file_writer << person_str << std::endl;
                    age = std::rand() % 101; // Random number between 0 and 100
                    person_str = "delete " + std::to_string(AliceDB::get_current_timestamp()) + " "  + name + " " + surname + " " +  std::to_string(age);
                    file_writer << person_str << std::endl;
                }
    }
    file_writer.close();

    AliceDB::Producer<Person> *prod = new AliceDB::FileProducer<Person>(file_name,parseLine);


    //AliceDB::Tuple<Person> *tpl = new AliceDB::Tuple<Person>;

    AliceDB::Node *source = new AliceDB::SourceNode<Person>(prod, 40);
    
    AliceDB::Queue *source_queue = source->Output();
    
    AliceDB::Node *name_getter = new AliceDB::ProjectionNode<Person, Name>(source, proj_names);
    
    AliceDB::Queue *name_queue = name_getter->Output();

    AliceDB::Node *sink = new AliceDB::SinkNode<Name>(name_getter);

    for(int i = 0; i < 100; i++){
        source->Compute();
        name_getter->Compute();
        sink->Compute();
    }

    AliceDB::SinkNode<Name> *real_sink = reinterpret_cast<AliceDB::SinkNode<Name>*>(sink);

    real_sink->Print(AliceDB::get_current_timestamp(), print_name);

    // check queue contents, make sure they are empty
    int count = 0;
    const char *data;
    while(source_queue->GetNext(&data)){count++;}
    while(name_queue->GetNext(&data)){count++;}
    ASSERT_EQ(count, 0);


    // delete file
    std::filesystem::remove("./people.txt");
}
*/

TEST(STATELESS_TEST, single_version_test_on_graph){

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it
    std::string file_name = "./people.txt";

    std::ofstream file_writer{file_name};


    for (auto &name : names){
            for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp()) + " "  + name + " " + surname + " " +  std::to_string(age);
                //std::cout << test_str <<std::endl;
                file_writer << person_str << std::endl;
            }
    }

    file_writer.close();


    AliceDB::Producer<Person> *prod = new AliceDB::FileProducer<Person>(file_name,parseLine);    
    AliceDB::WorkerPool *pool = new AliceDB::WorkerPool(1);
    AliceDB::Graph *g = new AliceDB::Graph();

    auto *view = 
        g->View(
            g->Projection(
                [](const Person &p) { return Name {.name=p.name}; },
                g->Filter(
                    [](const Person &p) -> bool {return p.age > 18 && p.name[0] == 'S' ;},
                    g->Source(prod,5)
                )
            )
        );

    pool->Start(g);

    // bussy wait to test if input will get updated
    int j = 0;
    while(j < 10000000){
        j++;
    }

    std::cout<<j<<std::endl;
    // make few iteration, to process whole data
    //g->Process(10);




    // debugging
    AliceDB::SinkNode<Name> *real_sink = reinterpret_cast<AliceDB::SinkNode<Name>*>(view);
    real_sink->Print(AliceDB::get_current_timestamp(), print_name );


    // delete file
    std::filesystem::remove("./people.txt");
}
