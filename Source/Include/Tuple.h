#ifndef ALICEDBTUPLE
#define ALICEDBTUPLE

#include <cstddef>

#include "Common.h"

namespace AliceDB {

// this will be used by cache to transfer data
template <typename Type>
struct Tuple {
  Delta delta;
  Type data;
};

}  // namespace AliceDB

#endif