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
void prepare_test_data_files(std::string people_fname, std::string dogs_fname){
    // Seed the random number generator
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it

    std::ofstream people_writter{people_fname};
    std::ofstream dog_writter{dogs_fname};
    
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
                if(cnt > 100){ break;}
        }
        if(cnt > 100){break;}
    }

    people_writter.close();

    // parse dogs
    for(int i = 0; i < 50; i++){
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

void print_joindogperson(const char *data){
    const JoinDogPerson *p = reinterpret_cast<const JoinDogPerson*>(data);

    std::cout<<p->name.data() << " " << p->surname.data() << " " << p->favourite_dog_race.data() << " " << p->dog_cost << " " << p->account_balace << " "  << p->age << std::endl; 
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

TEST(MULTINODE_TEST, multinode_test){
    std::string dogs_fname = "dogs.txt";
    std::string people_fname = "people.txt";
    prepare_test_data_files(people_fname, dogs_fname);



    // define new graph instance
    AliceDB::Graph *g = new AliceDB::Graph;

    // define 2 data producers
    AliceDB::Producer<Person> *prod_people = new AliceDB::FileProducer<Person>(people_fname,parsePerson);
    AliceDB::Producer<Dog> *prod_dogs = new AliceDB::FileProducer<Dog>(dogs_fname,parseDog);

    // create worker pool with single worker thread
    AliceDB::WorkerPool *pool = new AliceDB::WorkerPool(2);

    // define processing graph
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
                            g->Source(prod_people,0)
                        ),
                        g->Source(prod_dogs,0)
                    )
                    
                )
            )
        );
    // start processing data
    pool->Start(g);
    int i = 0;
    for(; i < 100000000; i++);
    std::cout<<i<<std::endl;


    AliceDB::SinkNode<CanAffordDog> *real_sink = reinterpret_cast<AliceDB::SinkNode<CanAffordDog>*>(view);

    real_sink->Print(AliceDB::get_current_timestamp(), print_canafforddog);

}