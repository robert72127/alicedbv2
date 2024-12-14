#ifndef ALICEDBDCOMMON
#define ALICEDBDCOMMON

#include <chrono>

namespace AliceDB {

#define PageSize 4096

using index = long long;

using timestamp = long long;

struct Delta{
	timestamp ts;
	int count;
};

// for automatically sorting by delta
bool deltaComparator(const Delta& a, const Delta& b) {
    return a.ts < b.ts;
}

/* use buffer pool and memory arena for queue as two separate memory pools

Starting: BufferPool start first then disk manager

Stopping: Stop buffer pool, send all write requests, then stop DiskManager

*/

template<typename T>
concept Arithmetic = std::is_arithmetic_v<T>;

timestamp get_current_timestamp(){
		auto now = std::chrono::system_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
		return duration.count();
}


} // namespace AliceDB

#endif