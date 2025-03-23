#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <vector>
#include <thread>

#include "gtest/gtest.h"

#include "AliceDB.h"

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
 

void print_name(const AliceDB::Tuple<Name> &current_tuple){
    const Name &p = current_tuple.data;
    std::cout<<p.name.data() << std::endl; 
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


std::vector<std::string> dummy_data; 

void generate_dummy_data(){
    for (auto &name : names){
            for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp()) + " "  + name + " " + surname + " " +  std::to_string(age);
            
                dummy_data.push_back(person_str);
            }
    }
}




const int PORT = 8080;
const char* HOST="127.0.0.1"; // use local host as ip address
const int BUFFER_SIZE = 1024;






void Server(std::vector<std::string> & messages, bool *stop){
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1){
        //std::cerr << "Failed to create server\n";
        return;
    }

    // bind the socket with ip and port
    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));

    address.sin_family = AF_INET; // ipv4
    address.sin_addr.s_addr = inet_addr(HOST); // ip addr
    address.sin_port = htons(PORT); // port in network byte order

    int opt = 1;
    //set socket options to allow reuse of the address and port
    if(setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
        //std::cerr<<"Failed to setsocket options\n";
        close(server_fd);
        return;
    }

    if(bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0 ){
        //std::cerr<<"Failed to bind\n";
        close(server_fd);
        return;
    }

    // listen for connections
    if(listen(server_fd,3) < 0){
        //std::cerr<<"Listen failed\n";
        close(server_fd);
        return;
    }

    //std::cout<< "Server listening on\nIP: " << HOST << "\nport: " << PORT<< std::endl;
    struct timeval timeout;
    timeout.tv_sec = 1;  // 1 second timeout
    timeout.tv_usec = 0;

    // accept and handle connections
    while(*stop == false){
        //std::cout << "Waiting for connections...\n";

        // Initialize file descriptor set
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(server_fd, &readfds);
        int max_fd = server_fd;

        // Set timeout
        struct timeval current_timeout = timeout;

        // Wait for activity on the server socket
        int activity = select(max_fd + 1, &readfds, NULL, NULL, &current_timeout);

        if (activity < 0 && errno != EINTR) {
            //std::cerr << "select error\n";
            break;
        }

        if (activity == 0) {
            //std::cout << "No incoming connections within timeout period.\n";
            // Perform other tasks or continue
            continue;
        }

        if (FD_ISSET(server_fd, &readfds)) {
            struct sockaddr_in client_address;
            socklen_t client_addr_len = sizeof(client_address);

            int new_socket = accept(server_fd, (struct sockaddr*)&client_address, &client_addr_len);
            if(new_socket < 0){
               //std::cerr << "Accept failed: " << strerror(errno) << "\n"; 
               continue;
            }

            //std::cout << "Connected to client: " << inet_ntoa(client_address.sin_addr)  << ":" << ntohs(client_address.sin_port) << "\n";

            for(auto &message: messages){
                // send length firs
                uint32_t message_len = htonl(message.length());
                //std::cout<< "MESSAGE LEN ON SERVER : " << message_len << std::endl;
                if (send(new_socket, (void *)&message_len,  sizeof(uint32_t), 0) < 0 ||
                    send(new_socket, message.c_str(), message.size(), 0) < 0) {
                    std::cerr << "Failed to send message to client.\n";
                    return;
                } else {
                    //std::cout << "Sent message to client: " << message <<std::endl;;
                }
            }

            close(new_socket);
        }

    }
    //std::cout<<"Shutting down the server thread\n";
    close(server_fd);

}

void Client(){
    int client_socket = socket(AF_INET, SOCK_STREAM,0);
    if(client_socket < 0){
        std::cerr << "Failed to create client socket\n";
        return;
    }

    struct sockaddr_in server_address;
    std::memset(&server_address, 0, sizeof(server_address));

    server_address.sin_family = AF_INET; // ipv4
    server_address.sin_port = htons(PORT); // set port

    // convert ip address from text to biarny form
    if(inet_pton(AF_INET, HOST, &server_address.sin_addr) <= 0){
        close(client_socket);
        std::cerr<<"Invalid address/ address not supported\n";
        return;
    }

    // connect to the server
    if(connect(client_socket, (struct sockaddr*)&server_address, sizeof(server_address)) < 0 ){
        std::cerr << "Connection to server failed\n";
        close(client_socket);
        return;
    }

    std::cout<<"Connected to server!\n";

    // receive data from the server
    char buffer[BUFFER_SIZE];
    
    std::string complete_message;

    while (true) {
        std::memset(buffer, 0, sizeof(buffer)); // Initialize buffer

        ssize_t bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received < 0) {
            std::cerr << "Failed to receive data from server.\n";
            break;
        } else if (bytes_received == 0) {
            // No more data; server has closed the connection
            std::cout << "Server closed the connection.\n";
            break;
        } else {
            // Append received data to the complete message
            complete_message += std::string(buffer, bytes_received);
        }
    }
    std::cout << "Complete message from server:\n" << complete_message << "\n";

    // 5. Close the connection
    close(client_socket);
    std::cout << "Connection closed.\n";

}


TEST(TCP_TEST, simple_tcp){

    generate_dummy_data();

    bool *stop = new bool;
    *stop = false;

    std::thread server_thread(Server, std::ref(dummy_data), stop);

    int i =0;
    for(;i<10000;i++);

    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();


    //Client();

    auto *view = 
        g->View(
            g->Projection(
                [](const Person &p) { return Name {.name=p.name}; },
                g->Filter(
                    [](const Person &p) -> bool {return p.age > 18 && p.name[0] == 'S' ;},
                    g->Source<Person>(AliceDB::ProducerType::TCPCLIENT , HOST, parseLine,0)
                )
            )
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopProcessing();


    // debugging
    
    for(auto it = view->begin() ; it != view->end(); ++it){
        print_name(*it);
    }
    

    *stop = true;
    if(server_thread.joinable())
        server_thread.join();
    delete stop;

    db = nullptr;
    std::filesystem::remove_all("database");
}