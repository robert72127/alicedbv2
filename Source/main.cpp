#include <iostream>
#include <fstream>

#include "Include/Node.h"
#include "Include/Graph.h"
#include "Include/Producer.h"
#include "Common.h"
#include "Tuple.h"

struct Person {
    char name[50];
    char surname[50];
    int age;
};

struct Name{
    char name[50]; 
};

bool filter_adult(const Person &p){
    return p.age > 18;
}

Name proj_names(const Person &p){
    Name N;
    std::memcpy(N.name, p.name, 50);
    return N;
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
        

int main(){
    /*
    AliceDB::Tuple<Person> *alice = new AliceDB::Tuple<Person>;
    std::string test_str = "insert " + std::to_string(AliceDB::get_current_timestamp()) +   " Alice Peczyk 25";
    parseLine(test_str, alice);
    std::cout<<alice->delta.ts << " " << alice->delta.count << " " <<alice->data.name << " " << alice->data.surname << " " << alice->data.age << std::endl; 
    */

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


     std::array<int, 100> randomNumbers;

    // Seed the random number generator
    std::srand(std::time(nullptr));

    // Fill the array with random numbers from 0 to 100
   // for (auto& number : randomNumbers) {
   //     number = std::rand() % 101; // Random number between 0 and 100
   // }


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

    AliceDB::Tuple<Person> *tpl = new AliceDB::Tuple<Person>;

    AliceDB::Node *source = new AliceDB::SourceNode<Person>(prod, 5);
    
    AliceDB::Node *adults = new AliceDB::FilterNode<Person>(source, filter_adult);
    
    AliceDB::Node *name_getter = new AliceDB::ProjectionNode<Person, Name>(source, proj_names);
 
    AliceDB::Node *sink = new AliceDB::SinkNode<Person>(name_getter);

    AliceDB::Node *unn = new AliceDB::UnionNode<Person>(source, source);

}