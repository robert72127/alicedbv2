#include <iostream>
#include <fstream>
#include <filesystem>

#include "gtest/gtest.h"

#include "Node.h"
#include "Graph.h"
#include "Producer.h"
#include "Common.h"
#include "Tuple.h"


struct Person {
    char name[50];
    char surname[50];
    int age;
};

struct CrossPerson {
    char lname[50];
    char lsurname[50];
    int lage;
    char rname[50];
    char rsurname[50];
    int rage;
};

struct DoubleNamedPerson {
    char lname[50];
    char lsurname[50];
    int lage;
    char rname[50];
    char rsurname[50];
};

struct NameCumAge {
    char name[50];
    int age;
};






struct Name{
    char name[50]; 
};

bool filter_adult(const Person &p){
    return p.age > 18;
}

void proj_names(Person *p, Name *out){
    std::memcpy(&out->name, &p->name, 50);
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

            // Assign parsed fields to *out.
            // Assuming Type has the fields in the same order:
            
            // Copy name to the char array field. Ensure no overflow:
            std::strncpy(p->data.name, name, sizeof(p->data.name));
            std::strncpy(p->data.surname, surname, sizeof(p->data.surname));
            p->data.age = age;
            p->delta.count = (insert_delete == "insert")? 1 : -1;
            p->delta.ts = ts;
            return true;
}
 
void print_people(const char *data){
    const Person *p = reinterpret_cast<const Person*>(data);

    std::cout<<p->name << " " << p->surname << " " << p->age << std::endl; 
}

void print_crosspeople(const char *data){
    const CrossPerson *p = reinterpret_cast<const CrossPerson*>(data);

    std::cout<<p->lname << " " << p->lsurname << " " << p->lage << "\t\t"; 
    std::cout<<p->rname << " " << p->rsurname << " " << p->rage << std::endl; 
} 

void print_doublenamed(const char *data){
    const CrossPerson *p = reinterpret_cast<const CrossPerson*>(data);

    std::cout<<p->lname << " " << p->lsurname << " " << p->lage << "\t\t"; 
    std::cout<<p->rname << " " << p->rsurname << " " << std::endl; 
} 

void print_namecumage(const char *data){
    const NameCumAge *p = reinterpret_cast<const NameCumAge*>(data);

    std::cout<<p->name << " " << p->age << " " << std::endl; 
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
TEST(SIMPLESTATE_TEST, single_insert_manual_test){

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it
    std::string file_name_1 = "./people1.txt";
    std::string file_name_2 = "./people2.txt";

    std::ofstream file_writer_1{file_name_1};
    std::ofstream file_writer_2{file_name_2};

    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp()) + " "  + name + " " + surname + " " +  std::to_string(age);
                //std::cout << test_str <<std::endl;
                file_writer_1 << person_str << std::endl;
                file_writer_2 << person_str << std::endl;
        }
    }

    file_writer_1.close();
    file_writer_2.close();


    AliceDB::Producer<Person> *prod_1 = new AliceDB::FileProducer<Person>(file_name_1,parseLine);
    AliceDB::Producer<Person> *prod_2 = new AliceDB::FileProducer<Person>(file_name_2,parseLine);


    //AliceDB::Tuple<Person> *tpl = new AliceDB::Tuple<Person>;

    AliceDB::Node *source_1 = new AliceDB::SourceNode<Person>(prod_1, 5);
    AliceDB::Node *source_2 = new AliceDB::SourceNode<Person>(prod_2, 5);
    AliceDB::Node *inter = new AliceDB::IntersectNode<Person>(source_1,source_2);    
    
    AliceDB::Queue *source_1_queue = source_1->Output();
    AliceDB::Queue *source_2_queue = source_2->Output();
    AliceDB::Queue *inter_queue = inter->Output();
    
    AliceDB::Node *sink = new AliceDB::SinkNode<Person>(inter);


    for(int i = 0; i < 1000; i++){
        source_1->Compute();
        source_2->Compute();
        inter->Compute();
        sink->Compute();
    }

    AliceDB::SinkNode<Person> *real_sink = reinterpret_cast<AliceDB::SinkNode<Person>*>(sink);

    real_sink->Print(AliceDB::get_current_timestamp(), print_people);

    // check queue contents, make sure they are empty
    int count = 0;
    const char *data;
    while(source_1_queue->GetNext(&data)){count++;}
    while(source_2_queue->GetNext(&data)){count++;}
    while(inter_queue->GetNext(&data)){count++;}
    ASSERT_EQ(count, 0);


   std::filesystem::remove("./people2.txt");
   std::filesystem::remove("./people1.txt");
}



TEST(SIMPLESTATE_TEST, union_graph){

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it
    std::string file_name_1 = "./people1.txt";
    std::string file_name_2 = "./people2.txt";

    std::ofstream file_writer_1{file_name_1};
    std::ofstream file_writer_2{file_name_2};

    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) + " "  + name + " " + surname + " " +  std::to_string(age);
                //std::cout << test_str <<std::endl;
                file_writer_1 << person_str << std::endl;
                file_writer_2 << person_str << std::endl;
        }
    }

    file_writer_1.close();
    file_writer_2.close();


    AliceDB::Producer<Person> *prod_1 = new AliceDB::FileProducer<Person>(file_name_1,parseLine);
    AliceDB::Producer<Person> *prod_2 = new AliceDB::FileProducer<Person>(file_name_2,parseLine);

    AliceDB::Graph *g = new AliceDB::Graph;
    
    auto *view =
        g->View<Person>( // or except or intersect
            g->Union<Person>(
                g->Source<Person>(prod_1, 0),
                g->Source<Person>(prod_2,0)
            )
        );



    g->Process(100);


    AliceDB::SinkNode<Person> *real_sink = reinterpret_cast<AliceDB::SinkNode<Person>*>(view);

    real_sink->Print(AliceDB::get_current_timestamp(), print_people);

   std::filesystem::remove("./people2.txt");
   std::filesystem::remove("./people1.txt");
}



TEST(SIMPLESTATE_TEST, cross_join_graph){

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it
    std::string file_name_1 = "./people1.txt";
    std::string file_name_2 = "./people2.txt";

    std::ofstream file_writer_1{file_name_1};
    std::ofstream file_writer_2{file_name_2};

    int cnt = 0;
    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) + " "  + name + " " + surname + " " +  std::to_string(age);
                //std::cout << test_str <<std::endl;
                file_writer_1 << person_str << std::endl;
                file_writer_2 << person_str << std::endl;
                cnt++;
                if(cnt > 100){ break;}
        }
        if(cnt > 100){break;}
    }

    file_writer_1.close();
    file_writer_2.close();


    AliceDB::Producer<Person> *prod_1 = new AliceDB::FileProducer<Person>(file_name_1,parseLine);
    AliceDB::Producer<Person> *prod_2 = new AliceDB::FileProducer<Person>(file_name_2,parseLine);

    AliceDB::Graph *g = new AliceDB::Graph;

    auto *view =
        g->View<CrossPerson>(
            g->CrossJoin<Person, Person, CrossPerson>(
                [](Person *left, Person *right, CrossPerson *out){
                    std::memcpy(&out->lname, &left->name, sizeof(left->name));
                    std::memcpy(&out->rname, &right->name, sizeof(right->name));

                    std::memcpy(&out->lsurname, &left->surname, sizeof(left->surname));
                    std::memcpy(&out->rsurname, &right->surname, sizeof(left->surname));
                    out->lage = left->age;
                    out->rage = right->age; 
                },
                g->Source<Person>(prod_1, 0),
                g->Source<Person>(prod_2,0)
            )
        );



    g->Process(100);


    AliceDB::SinkNode<CrossPerson> *real_sink = reinterpret_cast<AliceDB::SinkNode<CrossPerson>*>(view);

    real_sink->Print(AliceDB::get_current_timestamp(), print_crosspeople);

   std::filesystem::remove("./people2.txt");
   std::filesystem::remove("./people1.txt");
}


TEST(SIMPLESTATE_TEST, join_on_graph){

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it
    std::string file_name_1 = "./people1.txt";
    std::string file_name_2 = "./people2.txt";

    std::ofstream file_writer_1{file_name_1};
    std::ofstream file_writer_2{file_name_2};

    int cnt = 0;
    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) + " "  + name + " " + surname + " " +  std::to_string(age);
                //std::cout << test_str <<std::endl;
                file_writer_1 << person_str << std::endl;
                file_writer_2 << person_str << std::endl;
                cnt++;
                if(cnt > 100){ break;}
        }
        if(cnt > 100){break;}
    }

    file_writer_1.close();
    file_writer_2.close();


    AliceDB::Producer<Person> *prod_1 = new AliceDB::FileProducer<Person>(file_name_1,parseLine);
    AliceDB::Producer<Person> *prod_2 = new AliceDB::FileProducer<Person>(file_name_2,parseLine);

    AliceDB::Graph *g = new AliceDB::Graph;


    auto *view =
        g->View<DoubleNamedPerson>(
            g->Join<Person, Person, int,  DoubleNamedPerson>(
                [](Person *l, int *age)  {*age = l->age;},
                [](Person *r, int *age)  {*age = r->age;},
                [](Person *left, Person *right, DoubleNamedPerson *out){
                    std::memcpy(&out->lname, &left->name, sizeof(left->name));
                    std::memcpy(&out->rname, &right->name, sizeof(right->name));
                    std::memcpy(&out->lsurname, &left->surname, sizeof(left->surname));
                    std::memcpy(&out->rsurname, &right->surname, sizeof(left->surname));
                    out->lage = left->age;
                },
                g->Source<Person>(prod_1, 0),
                g->Source<Person>(prod_2,0)
            )
        );

    g->Process(100);


    AliceDB::SinkNode<DoubleNamedPerson> *real_sink = reinterpret_cast<AliceDB::SinkNode<DoubleNamedPerson>*>(view);

    real_sink->Print(AliceDB::get_current_timestamp(), print_doublenamed);

   std::filesystem::remove("./people2.txt");
   std::filesystem::remove("./people1.txt");
}
*/

TEST(SIMPLESTATE_TEST, join_on_graph){

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it
    std::string file_name = "./people1.txt";

    std::ofstream file_writer{file_name};

    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) + " "  + name + " " + surname + " " +  std::to_string(age);
                //std::cout << test_str <<std::endl;
                file_writer << person_str << std::endl;
        }
    }

    file_writer.close();


    AliceDB::Producer<Person> *prod_1 = new AliceDB::FileProducer<Person>(file_name,parseLine);

    AliceDB::Graph *g = new AliceDB::Graph;

    auto *view = 
        g->View<NameCumAge>(
            g->AggregateBy<Person,char,NameCumAge>(
               //aggregate function like sum etc
               [](NameCumAge *n, Person *p, int start ){ std::memcpy(n->name, p->name, sizeof(n->name));  n->age += p->age;},
                // group by function, specifies what to group on
                [](Person *in, char *on){std::memcpy(on, &in->name, sizeof(in->name)); },
                g->Source<Person>(prod_1, 0)
            )
        );
    

    g->Process(100);


    AliceDB::SinkNode<NameCumAge> *real_sink = reinterpret_cast<AliceDB::SinkNode<NameCumAge>*>(view);

    real_sink->Print(AliceDB::get_current_timestamp(), print_namecumage);

   std::filesystem::remove("./people.txt");
}


