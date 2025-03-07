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
    std::array<char, 50> name;
    std::array<char, 50> surname;
    int age;
};

struct CrossPerson {
    std::array<char, 50> lname;
    std::array<char, 50> lsurname;
    int lage;
    std::array<char, 50> rname;
    std::array<char, 50> rsurname;
    int rage;
};

struct DoubleNamedPerson {
    std::array<char, 50> lname;
    std::array<char, 50> lsurname;
    int lage;
    std::array<char, 50> rname;
    std::array<char, 50> rsurname;
};

struct NameCumAge {
    std::array<char, 50> name;
    int age;
};

struct Name{
    std::array<char, 50> name;
};

bool filter_adult(const Person &p){
    return p.age > 18;
}

void proj_names(Person *p, Name *out){
    std::memcpy(&out->name, &p->name, 50);
}

bool parseLine(std::istringstream &iss, Person *p) {

            char name[50];
            char surname[50];
            int age;

            if (!(iss >> name >> surname >> p->age)) {
                return false; // parse error
            }

            // Copy name to the char array field. Ensure no overflow:
            std::strncpy(p->name.data(), name, sizeof(p->name));
            std::strncpy(p->surname.data(), surname, sizeof(p->surname));
            
            return true;
}

 
void print_people(const char *data){
    const Person *p = reinterpret_cast<const Person*>(data);

    std::cout<<p->name.data() << " " << p->surname.data() << " " << p->age << std::endl; 
}

void print_crosspeople(const char *data){
    const CrossPerson *p = reinterpret_cast<const CrossPerson*>(data);

    std::cout<<p->lname.data() << " " << p->lsurname.data() << " " << p->lage << "\t\t"; 
    std::cout<<p->rname.data() << " " << p->rsurname.data() << " " << p->rage << std::endl; 
} 

void print_doublenamed(const char *data){
    const CrossPerson *p = reinterpret_cast<const CrossPerson*>(data);

    std::cout<<p->lname.data() << " " << p->lsurname.data() << " " << p->lage << "\t\t"; 
    std::cout<<p->rname.data() << " " << p->rsurname.data() << " " << std::endl; 
} 

void print_namecumage(const char *data){
    const NameCumAge *p = reinterpret_cast<const NameCumAge*>(data);

    std::cout<<p->name.data() << " " << p->age << " " << std::endl; 
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
        g->View( // or except or intersect
            g->Union(
                g->Source(prod_1, 0),
                g->Source(prod_2,0)
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
        g->View(
            g->CrossJoin(
                [](const Person &left, const Person &right){
                    return CrossPerson{
                        .lname=left.name,
                        .lsurname=left.surname,
                        .lage=left.age,
                        .rname=right.name,
                        .rsurname=right.surname,
                        .rage=right.age
                    };
                },
                g->Source(prod_1, 0),
                g->Source(prod_2,0)
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
        g->View(
            g->Join(
                [](const Person &l)  { return l.age;},
                [](const Person &r)  { return r.age;},
                [](const Person &left, const Person &right) { 
                    return DoubleNamedPerson {
                        .lname = left.name,
                        .lsurname = left.surname,
                        .lage = left.age,
                        .rname = right.name,
                        .rsurname = right.surname,
                    };
                },
                g->Source(prod_1, 0),
                g->Source(prod_2,0)
            )
        );

    /*
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
    */

    g->Process(100);


    AliceDB::SinkNode<DoubleNamedPerson> *real_sink = reinterpret_cast<AliceDB::SinkNode<DoubleNamedPerson>*>(view);

    real_sink->Print(AliceDB::get_current_timestamp(), print_doublenamed);

   std::filesystem::remove("./people2.txt");
   std::filesystem::remove("./people1.txt");
}

TEST(SIMPLESTATE_TEST, aggr_on_graphs){

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
        g->View(
            g->AggregateBy(
               //aggregate function like sum etc
               [](const Person &p, NameCumAge &n){  n.name = p.name, n.age+=p.age;},
               // group by function, specifies what to group on
               [](const Person &in){ return Name{in.name};},
                g->Source(prod_1, 0)
            )
        );
    

    g->Process(100);


    AliceDB::SinkNode<NameCumAge> *real_sink = reinterpret_cast<AliceDB::SinkNode<NameCumAge>*>(view);

    real_sink->Print(AliceDB::get_current_timestamp(), print_namecumage);

   std::filesystem::remove("./people.txt");
}
