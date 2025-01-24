#ifndef ALICEDBPRODUCER
#define ALICEDBPRODUCER

#include <arpa/inet.h>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <sstream>
#include <vector>
#include <functional>
#include <fstream>

#include "Tuple.h"

namespace AliceDB {

/** @brief abstract source producer, producer will provide buffered new input
 * this could represent wrapper around kafka source, network socket, or simple
 * file
 */

template <typename Type>
class Producer {
 public:
  using value_type = Type;

  Producer() = default;
  virtual ~Producer() = default;

  /** @brief fill storage with next input if data is available
   * @return true if storage was set, false if there were no new values at the
   * current moment
   */
  virtual bool next(Tuple<Type> *storage) = 0;
};

// assumes format:
// insert/delete | timestamp | struct fields
template <typename Type>
class FileProducer : public Producer<Type> {
 public:
  // produce's data from strings in file
  FileProducer(const std::string &filename,
               std::function<bool(std::istringstream &, Type *)> parse)
      : current_line_(0), parse_{parse} {



    file_stream_.open(filename);
    if (!file_stream_.is_open()) {
      throw std::runtime_error("Failed to open file: " + filename);
    }
  }

  ~FileProducer() override {
    if (file_stream_.is_open()) {
      file_stream_.close();
    }
  }

  bool next(Tuple<Type> *storage) override {
    if (!file_stream_.is_open()) {
      return false;
    }

    std::string line;
    if (!std::getline(file_stream_, line)) {
      return false;  // no more data
    }

    ++current_line_;

    // failed to parse

    std::istringstream iss(line);
    AliceDB::timestamp ts;
    std::string insert_delete;

    if (!(iss >> insert_delete >> ts)) {
      return false;  // parse error
    }

    storage->delta.count = (insert_delete == "insert") ? 1 : -1;
    storage->delta.ts = ts;

    return this->parse_(iss, &storage->data);
  }

 private:
  /**
   * @brief Parse a line into the given Type.
   * Since Type is trivial, you can use either:
   * - std::istringstream with operator>> if Type fields are individually
   * readable.
   * - sscanf if you know the exact format string.
   */

  // std::function<bool(std::string, Tuple<Type> *)> produce_;
  std::function<bool(std::istringstream &, Type *)> parse_;
  std::ifstream file_stream_;
  unsigned long current_line_;
};


// assumes format:
// insert/delete | timestamp | struct fields
template <typename Type>
class TCPClientProducer : public Producer<Type> {
 public:
  // produce's data reading data from tcp socket
  TCPClientProducer(std::string IP_ADDR,
      std::function<bool(std::istringstream &, Type *)> produce)
      : produce_{produce}{
        
    int port = 8080;
    this->client_socket_ = socket(AF_INET, SOCK_STREAM,0);
    if(client_socket_ < 0){
        throw std::runtime_error("Failed to create client socket\n");
    }

    struct sockaddr_in server_address;
    std::memset(&server_address, 0, sizeof(server_address));

  
    server_address.sin_family = AF_INET; // ipv4
    server_address.sin_port = htons(port); // set port

    // convert ip address from text to biarny form
    if(inet_pton(AF_INET, IP_ADDR.c_str(), &server_address.sin_addr) <= 0){
        close(this->client_socket_);
        throw std::runtime_error("Invalid address/ address not supported\n");
        return;
    }

    // connect to the server
    if(connect(this->client_socket_, (struct sockaddr*)&server_address, sizeof(server_address)) < 0 ){
        close(this->client_socket_);
        throw std::runtime_error("Connection to server failed\n");
    }
    
  }

  ~TCPClientProducer() override {
    if (this->client_socket_ >= 0){
      close(this->client_socket_);
    }
  }


  bool next(Tuple<Type> *storage) override {
    uint32_t message_length;

    // prefixed with message length 
    ssize_t bytes_received = RecvMessage(&message_length, sizeof(message_length));
        if (bytes_received != sizeof(message_length)) {
            return false;
    }
    message_length = ntohl(message_length);
    if (message_length == 0) {
            return false;
    }

    // Receive the actual message data
    std::vector<char> buffer(message_length);
    bytes_received = RecvMessage(buffer.data(), message_length);
    if (bytes_received != static_cast<ssize_t>(message_length)) {
        return false;
    }

    std::string line(buffer.begin(), buffer.end());
    std::istringstream iss(line);
    AliceDB::timestamp ts;
    std::string insert_delete;


    if (!(iss >> insert_delete >> ts)) {
      return false;  // parse error
    }

    storage->delta.count = (insert_delete == "insert") ? 1 : -1;
    storage->delta.ts = ts;

    return this->produce_(iss, &storage->data);
  }

 private:

    ssize_t RecvMessage(void* buffer, size_t length) {
        size_t total_received = 0;
        char* buf = static_cast<char*>(buffer);
        while (total_received < length) {
            ssize_t bytes = recv(client_socket_, buf + total_received, length - total_received, 0);
            if (bytes <= 0) {
                return -1; 
            } 

            total_received += bytes;
        }
        return total_received;
    }


  int client_socket_;
  std::function<bool(std::istringstream &, Type *)> produce_;
};
}  // namespace AliceDB

#endif