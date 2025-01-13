#include <iostream>
#include <string>

#include "Graph.h"



int main(){


    std::string alicedb_intro = R"(
    AliceDB is library, which let's you define relational algebra mappings on your online data,
    and provide You with internaly and eventualy consistent view of results.
    )";

    std::cout << alicedb_intro << std::endl;

    std::cout << "This is example demo app explaining how to use library\n";

    std::string alicedb_usage_summary = R"(
    AliceDB operates on Cpp structs as data format. 
    It's user responsilibity to define right struct's that will correctly map his data. 


    To integrate AliceDB relational graphs into your application you need to define three things:

    1)  First you need to know where your data comes from and how it's structured,
        for that you need to pick correct Producer from AliceDB/Source/Include/Producer.h.

        AliceDB::Producer<Person> *prod = new AliceDB::FileProducer<Person>(file_name,parseLine);

        is example of producer that reads from file named file_name, and parse's arriving data into struct using parseLine function
        and here is definition of function itself:

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
 
    2) You need to define structure of your input and immediate computations as Cpp structs with few caveats:
    you can only use cpp primitive types + array type as struct fields

    struct Person {
        std::array<char,50> name;
        std::array<char,50> surname;
        int age;
    };

    struct Name{
        std::array<char,50> name;
    };


    3) Finally you can define your computations

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

    after whole computational graph is defined call:
    to start incremental computation

    g->process()



    )";


}