#ifndef _WATCH_H_
#define _WATCH_H_

namespace sharkstore {
namespace dataserver {
namespace watch {

typedef int64_t WatcherId; // socket session id
typedef std::string Key;
typedef std::string Prefix;
typedef std::string WatcherKey;
typedef std::string PrefixKey;

enum class WatchCode {
    kOK = 0,
    kKeyExist,
    kKeyNotExist,
    kWatcherExist,
    kWatcherNotExist,
    kWatcherNotNeed,
};

enum class WatchType {
    kKey = 0,
    kPrefix = 1,
};

}
}
}

#endif
