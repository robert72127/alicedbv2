#ifndef ALICEDBDCOMMON
#define ALICEDBDCOMMON

#include <chrono>

namespace AliceDB {

#define PageSize 4096

using index = long long;

using timestamp = unsigned long long;

struct Delta {
  timestamp ts;
  int count;
};

// for automatically sorting by delta
struct DeltaComparator {
  bool operator()(const Delta &a, const Delta &b) const {
    return a.ts < b.ts;  // Sort based on the timestamp
  }
};

/* use buffer pool and memory arena for queue as two separate memory pools

Starting: BufferPool start first then disk manager

Stopping: Stop buffer pool, send all write requests, then stop DiskManager

*/

template <typename T>
concept Arithmetic = std::is_arithmetic_v<T>;

timestamp get_current_timestamp() {
  auto now = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
  return duration.count();
}

}  // namespace AliceDB

#endif