#include <iostream>
#include <fstream>
#include <filesystem>
#include <memory>
#include <chrono>

#include "gtest/gtest.h"


#include "AliceDB.h"
// some dummy data

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


std::array<std::string, 50> dogbreeds = {
        "Labrador", "German_Shepherd", "Golden_Retriever", "Bulldog",
        "Beagle", "Poodle", "Rottweiler", "Yorkshire_Terrier", "Boxer", 
        "Dachshund", "Siberian_Husky", "Chihuahua", "Great_Dane", "Shih_Tzu",
        "Doberman_Pinscher", "Australian_Shepherd", "Cocker_Spaniel", "Pomeranian",
        "Boston_Terrier", "Shetland_Sheepdog", "Bernese_Mountain_Dog", "Havanese",
        "Cavalier_King_Charles_Spaniel", "Maltese", "Weimaraner", "Collie", 
        "Bichon_Frise", "English_Springer_Spaniel", "Papillon", "Saint_Bernard",
        "Bullmastiff", "Akita", "Samoyed", "Bloodhound", "Alaskan_Malamute",
        "Newfoundland", "Border_Collie", "Vizsla", "Australian_Cattle_Dog",
        "West_Highland_White_Terrier", "Rhodesian_Ridgeback", "Chow_Chow",
        "Shiba_Inu", "Basset_Hound", "Bulldog", "Irish_Setter",
        "Whippet", "Scottish_Terrier", "Italian_Greyhound", "American_Eskimo_Dog"
    };

std::array<float, 50> dogprices = {
        1000.0, 1200.0, 1100.0, 1300.0,
        900.0, 1500.0, 1400.0, 1250.0, 1000.0, 
        850.0, 2000.0, 800.0, 2500.0, 950.0,
        1350.0, 1450.0, 1050.0, 1150.0, 1000.0, 
        950.0, 1800.0, 1200.0, 1150.0, 900.0,
        1100.0, 1000.0, 1250.0, 1350.0, 1500.0,
        2500.0, 2200.0, 2000.0, 1700.0, 1800.0,
        1600.0, 1400.0, 1250.0, 1100.0, 1300.0,
        1400.0, 1200.0, 950.0, 1100.0, 1500.0,
        1400.0, 1000.0, 900.0, 750.0, 620, 800.0
};

// this isn't required part, but we just need to create our data files
void prepare_people_data_file(std::string people_fname){
    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it

    std::ofstream people_writter{people_fname};
    
    // parse people
    int cnt = 0;
    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = 50;// std::rand() % 101; // Random number between 0 and 100
                
                int dog_race_nr = 1; //std::rand() % 50;
                
                float account_ballance = 100;// (std::rand() / (float)RAND_MAX) * 2000.0f;

                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
                //std::cout << test_str <<std::endl;
                people_writter << person_str << std::endl;
                cnt++;
                if(cnt > 1){ break;}
        }
        if(cnt > 100){break;}
    }

    people_writter.close();
}

// this isn't required part, but we just need to create our data files
void prepare_people_data_file_random(std::string people_fname){
    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it

    std::ofstream people_writter{people_fname};
    
    // parse people
    int cnt = 0;
    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                
                int dog_race_nr = std::rand() % 50;
                
                float account_ballance =  (std::rand() / (float)RAND_MAX) * 2000.0f;

                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
                //std::cout << test_str <<std::endl;
                people_writter << person_str << std::endl;
                cnt++;
                if(cnt > 1){ break;}
        }
        if(cnt > 100){break;}
    }

    people_writter.close();
}


void prepare_dog_data_file(std::string dogs_fname){

    std::ofstream dog_writter{dogs_fname};
    // parse dogs
    for(int i = 0; i < 5; i++){
        std::string breed = dogbreeds[i];
        float price = dogprices[i];
        std::string dog_str =  "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  +  breed + " "  +  std::to_string(price) ;

        dog_writter << dog_str << std::endl;
    }
    dog_writter.close();
}




// define our input data

struct Person {
    std::array<char, 50> name;
    std::array<char, 50> surname;
    std::array<char, 50> favourite_dog_race;
    int age;
    float account_balance;
};

struct NameTotalBalance {
    std::array<char, 50> name;
    float account_balance;
};

struct Name{
    std::array<char, 50> name;
};

struct Dog {
    std::array<char, 50> name;
    float cost;
};

// this is least nice part, we need to define transitional state structs

struct JoinDogPerson {
    std::array<char, 50> name;
    std::array<char, 50> surname;
    std::array<char, 50> favourite_dog_race;
    float dog_cost;
    float account_balace;
    int age;
};

struct PairPeople {
    std::array<char, 50> lname;
    std::array<char, 50> lsurname;
    int lage;
    std::array<char, 50> rname;
    std::array<char, 50> rsurname;
    int rage;
};


struct SameAgedPeople {
    std::array<char, 50> lname;
    std::array<char, 50> lsurname;
    int age;
    std::array<char, 50> rname;
    std::array<char, 50> rsurname;
};



void print_person(const char *data){
    const Person *p = reinterpret_cast<const Person*>(data);
    std::cout<<p->name.data() << " " << p->surname.data() << " " << p->favourite_dog_race.data() << " " << p->age << " " << p->account_balance  << std::endl; 
} 


void print_joindogperson(const char *data){
    const JoinDogPerson *p = reinterpret_cast<const JoinDogPerson*>(data);

    std::cout<<p->name.data() << " " << p->surname.data() << " " << p->favourite_dog_race.data() << " " << p->dog_cost << " " << p->account_balace << " "  << p->age << std::endl; 
} 

void print_nametotalbalance(const char *data){
    const NameTotalBalance *p = reinterpret_cast<const NameTotalBalance*>(data);
    std::cout<<p->name.data() << " " << p->account_balance << std::endl; 
} 




/// and final state
struct CanAffordDog{
    std::array<char, 50> name;
    std::array<char, 50> surname;
};

void print_canafforddog(const char *data){
    const CanAffordDog *p = reinterpret_cast<const CanAffordDog*>(data);

    std::cout<<p->name.data() << " " << p->surname.data()  << std::endl; 
} 



// we need to provide parseLine function to specify how to parse structs from producer into source, we might change api

// note this could be simplified with reflection mechanism 
bool parsePerson(std::istringstream &iss, Person *p) {

            char name[50];
            char surname[50];
            char favourite_dog_race[50];

            if (!(iss >> name >> surname >> favourite_dog_race >> p->age >> p->account_balance )) {
                return false; // parse error
            }

            // Copy fields to ensure no overflow:
            std::strncpy(p->name.data(), name, sizeof(p->name));
            std::strncpy(p->surname.data(), surname, sizeof(p->surname));
            std::strncpy(p->favourite_dog_race.data(), favourite_dog_race, sizeof(p->favourite_dog_race));

//            std::cout << (char*)p << std::endl; 
            return true;
}

bool parseDog(std::istringstream &iss, Dog *d) {

            char name[50];

            if (!(iss >> name >> d->cost )) {
                return false; // parse error
            }

            // Copy fields to ensure no overflow:
            std::strncpy(d->name.data(), name, sizeof(d->name));
            return true;
}

TEST(STATEFULL_TEST, UNION){

    std::string people_fname = "people.txt";
    std::string people_fname2 = "people2.txt";
    prepare_people_data_file(people_fname);
    prepare_people_data_file(people_fname2);
    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
                g->Union(
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0),
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname2, parsePerson,0)
                )
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);


    // debugging
    AliceDB::SinkNode<Person> *real_sink = reinterpret_cast<AliceDB::SinkNode<Person>*>(view);
    real_sink->Print(AliceDB::get_current_timestamp(), print_person );

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}

TEST(STATEFULL_TEST, EXCEPT){

    std::string people_fname = "people.txt";
    std::string people_fname2 = "people2.txt";
    prepare_people_data_file_random(people_fname);
    prepare_people_data_file_random(people_fname2);
    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
                g->Except(
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0),
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname2, parsePerson,0)
                )
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);


    // debugging
    AliceDB::SinkNode<Person> *real_sink = reinterpret_cast<AliceDB::SinkNode<Person>*>(view);
    real_sink->Print(AliceDB::get_current_timestamp(), print_person );

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}

TEST(STATEFULL_TEST, INTERSECT){

    std::string people_fname = "people.txt";
    std::string people_fname2 = "people2.txt";
    prepare_people_data_file_random(people_fname);
    prepare_people_data_file_random(people_fname2);
    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
                g->Intersect(
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0),
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname2, parsePerson,0)
                )
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);


    // debugging
    AliceDB::SinkNode<Person> *real_sink = reinterpret_cast<AliceDB::SinkNode<Person>*>(view);
    real_sink->Print(AliceDB::get_current_timestamp(), print_person );

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}

void print_pairpeople(const char *data){
    const PairPeople *p = reinterpret_cast<const PairPeople*>(data);

    std::cout<<p->lname.data() << " " << p->lsurname.data() << " " << p->lage << "\t\t"; 
    std::cout<<p->rname.data() << " " << p->rsurname.data() << " " << p->rage << std::endl; 
} 

TEST(SIMPLESTATE_TEST, CROSSJOIN){

    std::string people_fname = "people.txt";
    std::string people_fname2 = "people2.txt";
    prepare_people_data_file_random(people_fname);
    prepare_people_data_file_random(people_fname2);
    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();


    auto *view =
        g->View(
            g->CrossJoin(
                [](const Person &left, const Person &right){
                    return PairPeople{
                        .lname=left.name,
                        .lsurname=left.surname,
                        .lage=left.age,
                        .rname=right.name,
                        .rsurname=right.surname,
                        .rage=right.age
                    };
                },
                g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0),
                g->Source<Person>(AliceDB::ProducerType::FILE , people_fname2, parsePerson,0)
            )
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);

    AliceDB::SinkNode<PairPeople> *real_sink = reinterpret_cast<AliceDB::SinkNode<PairPeople>*>(view);
    real_sink->Print(AliceDB::get_current_timestamp(), print_pairpeople);

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
   
}



void print_sameagedpeople(const char *data){
    const SameAgedPeople *p = reinterpret_cast<const SameAgedPeople*>(data);
    std::cout<<p->lname.data() << " " << p->lsurname.data() << " " << p->age << " "<< p->rname.data() << " " << p->rsurname.data()  << std::endl; 
} 

TEST(SIMPLESTATE_TEST, JOIN){

    std::string people_fname = "people.txt";
    std::string people_fname2 = "people2.txt";
    prepare_people_data_file_random(people_fname);
    prepare_people_data_file_random(people_fname2);
    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view =
        g->View(
            g->Join(
                [](const Person &l)  { return l.age;},
                [](const Person &r)  { return r.age;},
                [](const Person &left, const Person &right) { 
                    return SameAgedPeople {
                        .lname = left.name,
                        .lsurname = left.surname,
                        .age = left.age,
                        .rname = right.name,
                        .rsurname = right.surname,
                    };
                },
                g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0),
                g->Source<Person>(AliceDB::ProducerType::FILE , people_fname2, parsePerson,0)
            )
        );


    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);

    AliceDB::SinkNode<SameAgedPeople> *real_sink = reinterpret_cast<AliceDB::SinkNode<SameAgedPeople>*>(view);
    real_sink->Print(AliceDB::get_current_timestamp(), print_sameagedpeople);

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}


TEST(STATEFULL_TEST, AGGREGATEBY){

    std::string people_fname = "people.txt";
    std::string people_fname2 = "people2.txt";
    prepare_people_data_file(people_fname);
    prepare_people_data_file(people_fname2);
    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
                g->AggregateBy(
                    [](const Person &p) { return Name{.name = p.name}; },
                    [](const Person &p, int count, const NameTotalBalance &nb, bool first ){
                        return NameTotalBalance {
                            .name = p.name,
                            .account_balance = first?  p.account_balance : p.account_balance + nb.account_balance
                        };
                    },
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
                )
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);


    // debugging
    AliceDB::SinkNode<NameTotalBalance> *real_sink = reinterpret_cast<AliceDB::SinkNode<NameTotalBalance>*>(view);
    real_sink->Print(AliceDB::get_current_timestamp(), print_nametotalbalance );

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}
