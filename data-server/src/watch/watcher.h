#ifndef _WATCHER_H_
#define _WATCHER_H_

#include <mutex>
#include <unordered_map>
#include <atomic>

#include "watch.h"
#include "common/socket_session.h"
#include "storage/store.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

class Watcher {
public:
    Watcher(uint64_t tableID, std::vector<std::string>&& keys, uint64_t client_version,
            int64_t expire_time, common::ProtoMessage* msg, WatchType type = WatchType::kKey);
    ~Watcher();

    void setBufferFlag(int64_t flag) { buffer_flag_ = flag; }
    int64_t getBufferFlag() const { return buffer_flag_; }

    uint64_t GetTableId() { return table_id_; }

    const std::vector<std::string>& GetKeys() const { return keys_; }

    const std::string& GetFirstKey() const {
        assert(!keys_.empty());
        return keys_[0];
    }

    common::ProtoMessage* GetMessage() const { return message_; }

    WatchType GetType() const { return type_; }

    void SetWatcherId(WatcherId id) { watcher_id_ = id; }
    WatcherId GetWatcherId() const { return watcher_id_; }

    int64_t GetExpireTime() const { return expire_time_; }

    bool IsSentResponse() const {
        std::lock_guard<std::mutex> lock(send_lock_);
        return response_sent_;
    }

    int64_t getKeyVersion() const { return key_version_; }
    int64_t GetSessionId() const{ return session_id_; }
    int64_t GetMsgId() const { return msg_id_; }

    void Send(google::protobuf::Message* resp);

private:
    uint64_t table_id_ = 0;
    std::vector<std::string> keys_;
    int64_t key_version_ = 0;
    int64_t expire_time_ = 0;
    common::ProtoMessage* message_ = nullptr;
    WatchType type_ = WatchType::kKey;
    WatcherId watcher_id_ = 0;
    int64_t session_id_ = 0;
    int64_t msg_id_ = 0;
    //prefix mode:
    // 0 key has no changing, need to add watcher
    // -1 key version is lower than buffer or buffer is empty, need to get all from db
    int64_t buffer_flag_ = 0;

    mutable std::mutex send_lock_;
    bool response_sent_ = false;
};

typedef std::shared_ptr<Watcher> WatcherPtr;

template <class T>
struct Greater {
    bool operator()(const T& a, const T& b) {
        return a->GetExpireTime() > b->GetExpireTime();
    }
};


} // namespace watch
}
}

#endif
