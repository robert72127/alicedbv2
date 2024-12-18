#ifndef ALICEDBNODEWRAPPERS
#define ALICEDBNODEWRAPEERS


/**
 * set of wrappers and helpers for Node creation 
 */
namespace AliceDB
{


#define on(Type, condition) [&](const Type& t) { return condition; }


}; // namespace AliceDB



#endif