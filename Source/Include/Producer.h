#ifndef ALICEDBPRODUCER
#define ALICEDBPRODUCER

#include <functional>
#include <string>

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
               std::function<bool(std::string, Tuple<Type> *)> produce)
      : current_line_(0), produce_{produce} {
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
    if (!this->produce_(line, storage)) {
      return false;
    }

    return true;
  }

 private:
  /**
   * @brief Parse a line into the given Type.
   * Since Type is trivial, you can use either:
   * - std::istringstream with operator>> if Type fields are individually
   * readable.
   * - sscanf if you know the exact format string.
   */

  std::function<bool(std::string, Tuple<Type> *)> produce_;
  std::ifstream file_stream_;
  unsigned long current_line_;
};

}  // namespace AliceDB

#endif