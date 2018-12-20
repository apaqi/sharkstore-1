#ifndef _WATCH_SERVER_
#define _WATCH_SERVER_

#include <vector>
#include <mutex>

#include "watcher_set.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

#define WATCHER_SET_COUNT_MIN   1
#define WATCHER_SET_COUNT_MAX   64

class WatchServer {
public:
    WatchServer() = default;
    explicit WatchServer(uint64_t watcher_set_count);
    WatchServer(const WatchServer&) = delete;
    WatchServer& operator=(const WatchServer&) = delete;
    ~WatchServer();

    WatchCode AddKeyWatcher(WatcherPtr&, storage::Store *);
    WatchCode AddPrefixWatcher(WatcherPtr&, storage::Store *);

    WatchCode DelKeyWatcher(WatcherPtr&);
    WatchCode DelPrefixWatcher(WatcherPtr&);

    WatcherSet* GetWatcherSet(const std::string& first_key);

private:
    uint64_t                    watcher_set_count_ = WATCHER_SET_COUNT_MIN;
    std::vector<WatcherSet*>    watcher_set_list;
};


} // namespace watch
}
}
#endif
