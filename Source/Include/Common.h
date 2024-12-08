#ifndef ALICEDBDCOMMON
#define ALICEDBDCOMMON
namespace AliceDB {

#define PageSize 4096

using index = long long;

using timestamp = long long;

// for automatically sorting by delta
bool deltaComparator(const Delta& a, const Delta& b) {
    return a.ts < b.ts;
}

struct Delta{
	timestamp ts;
	int count;
};


/* use buffer pool and memory arena for queue as two separate memory pools

Starting: BufferPool start first then disk manager

Stopping: Stop buffer pool, send all write requests, then stop DiskManager

*/
} // namespace AliceDB

#endif