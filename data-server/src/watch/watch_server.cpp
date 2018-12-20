#include "watch_server.h"
#include "frame/sf_logger.h"
#include "common/ds_encoding.h"
#include "watch_util.h"

namespace sharkstore {
namespace dataserver {
namespace watch {



WatchServer::WatchServer(uint64_t watcher_set_count): watcher_set_count_(watcher_set_count) {
    watcher_set_count_ = watcher_set_count_ > WATCHER_SET_COUNT_MIN ? watcher_set_count_ : WATCHER_SET_COUNT_MIN;
    watcher_set_count_ = watcher_set_count_ < WATCHER_SET_COUNT_MAX ? watcher_set_count_ : WATCHER_SET_COUNT_MAX;

    for (uint64_t i = 0; i < watcher_set_count_; ++i) {
        watcher_set_list.push_back(new WatcherSet());
    }
}

WatchServer::~WatchServer() {
    for (auto watcher_set: watcher_set_list) {
       delete(watcher_set);
    }
}

WatcherSet* WatchServer::GetWatcherSet(const std::string& key) {
    std::size_t hash = std::hash<std::string>{}(key);
    return watcher_set_list[hash % watcher_set_count_];
}

WatchCode WatchServer::AddKeyWatcher(WatcherPtr& w_ptr, storage::Store *store_) {
    assert(w_ptr->GetType() == WatchType::kKey);

    FLOG_DEBUG("watch server ready to add key watcher: session_id [%" PRIu64 "] watch_id[%" PRIu64 "] key:%s",
               w_ptr->GetSessionId(), w_ptr->GetWatcherId(), EncodeToHexString(w_ptr->GetFirstKey()).c_str());

    auto wset = GetWatcherSet(w_ptr->GetFirstKey());
    w_ptr->SetWatcherId(wset->GenWatcherId());

    std::string encode_key;
    EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());
    return wset->AddKeyWatcher(encode_key, w_ptr, store_);
}

WatchCode WatchServer::AddPrefixWatcher(WatcherPtr& w_ptr, storage::Store *store_) {
    assert(w_ptr->GetType() == WatchType::kPrefix);

    FLOG_DEBUG("watch server add prefix watcher: session_id [%" PRIu64 "]", w_ptr->GetSessionId());

    auto ws = GetWatcherSet(w_ptr->GetFirstKey());
    w_ptr->SetWatcherId(ws->GenWatcherId());

    std::string encode_key;
    EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());
    return ws->AddPrefixWatcher(encode_key, w_ptr, store_);
}

WatchCode WatchServer::DelKeyWatcher(WatcherPtr& w_ptr) {
    FLOG_DEBUG("watch server del key watcher: watch_id [%" PRIu64 "]", w_ptr->GetWatcherId());

    assert(w_ptr->GetType() == WatchType::kKey);

    auto ws = GetWatcherSet(w_ptr->GetFirstKey());

    std::string encode_key;
    EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());
    return ws->DelKeyWatcher(encode_key, w_ptr->GetWatcherId());
}

WatchCode WatchServer::DelPrefixWatcher(WatcherPtr& w_ptr) {
    assert(w_ptr->GetType() == WatchType::kPrefix);

    FLOG_DEBUG("watch server del prefix watcher: watch_id [%" PRIu64 "]", w_ptr->GetWatcherId());

    auto ws = GetWatcherSet(w_ptr->GetFirstKey());

    std::string encode_key;
    EncodeKey(&encode_key, w_ptr->GetTableId(), w_ptr->GetKeys());
    return ws->DelPrefixWatcher(encode_key, w_ptr->GetWatcherId());
}

} // namepsace watch
}
}
