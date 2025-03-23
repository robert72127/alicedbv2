#include <array>
#include <string>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>

#include "AliceDB.h"

struct Person {
    std::array<char, 50> name;
    std::array<char, 50> surname;
    int age;
    float account_balance;
};


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



void print_person(const AliceDB::Tuple<Person> &current_tuple) {
    std::cout << current_tuple.delta.count << " " 
              << current_tuple.data.name.data() << " " 
              << current_tuple.data.surname.data() << " " 
              << current_tuple.data.age << " " 
              << current_tuple.data.account_balance  << std::endl; 
}


void prepare_test_data_file_binary(const std::string &bin_fname) {
    std::ofstream bin_out(bin_fname, std::ios::binary);
    if (!bin_out.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + bin_fname);
    }

    for (int i = 0; i < 100; ++i) {
        AliceDB::Tuple<Person> tuple;
        std::memset(&tuple, 0, sizeof(AliceDB::Tuple<Person>));
        
        tuple.delta.count = 1;
        tuple.delta.ts = AliceDB::get_current_timestamp();

        std::string name_str = names[i % names.size()];
        std::string surname_str = surnames[i % surnames.size()];

        std::strncpy(tuple.data.name.data(), name_str.c_str(), tuple.data.name.size());
        std::strncpy(tuple.data.surname.data(), surname_str.c_str(), tuple.data.surname.size());
        tuple.data.age = 18 + i;
        tuple.data.account_balance = 100.0f + i;

        bin_out.write(reinterpret_cast<char*>(&tuple.delta), sizeof(tuple.delta));
        bin_out.write(reinterpret_cast<char*>(&tuple.data), sizeof(Person));
    }
    bin_out.close();
}


TEST(BINARY_DATA_TEST, FILTER){
     std::string bin_fname = "people.bin";
    
    // Create binary file with test data.
    prepare_test_data_file_binary(bin_fname);
    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();


    auto *view = 
        g->View(
                g->Filter(
                    [](const Person &p) -> bool {return p.age > 18  ;},
                    g->Source<Person>(AliceDB::ProducerType::FILE_BINARY, bin_fname,0)
                )
        );


    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    db->StopProcessing();


    // debugging
    for(auto it = view->begin() ; it != view->end(); ++it){
        print_person(*it);
    }


    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}

