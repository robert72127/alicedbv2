#ifndef ALICEDBTUPLE
#define ALICEDBTUPLE

#include "Common.h"

#include <cstddef>

namespace AliceDB {

// this will be used by cache to transfer data
template <typename Type>
struct Tuple {
	Delta delta;
	Type data;
};

} // namespace AliceDB

#endif