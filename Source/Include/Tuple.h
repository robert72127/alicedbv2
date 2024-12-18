#ifndef ALICEDBTUPLE
#define ALICEDBTUPLE

#include <cstddef>

#include "Common.h"

namespace AliceDB {

// this will be used by queue to transfer data
template <typename Type>
struct Tuple {
  Delta delta;
  Type data;
};

}  // namespace AliceDB

#endif