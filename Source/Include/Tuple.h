#ifndef ALICEDBTUPLE
#define ALICEDBTUPLE

#include "Common.h"

#include <cstddef>

namespace AliceDB {

template <typename Type>

// this will be used by queue to transfer data
struct Tuple {
	Delta delta;
	Type data;
};

} // namespace AliceDB

#endif