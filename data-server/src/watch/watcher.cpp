#include "watcher.h"

#include "common/socket_session_impl.h"
#include "common/ds_encoding.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

Watcher::Watcher(uint64_t tableID, std::vector<std::string>&& keys, uint64_t client_version,
        int64_t expire_time, common::ProtoMessage* msg, WatchType type) :
    table_id_(tableID),
    keys_(std::move(keys)),
    key_version_(client_version),
    expire_time_(expire_time),
    message_(msg),
    type_(type),
    watcher_id_(msg->session_id),
    session_id_(msg->session_id),
    msg_id_(msg->header.msg_id) {
    assert(!keys.empty());
}

Watcher::~Watcher() {
    assert(message_ == nullptr);
}

void Watcher::Send(google::protobuf::Message* resp) {
    std::lock_guard<std::mutex> lock(send_lock_);
    if (response_sent_) {
        return;
    }

    uint32_t take_time = get_micro_second() - message_->begin_time;

    FLOG_DEBUG("before send, session_id: %" PRId64 ",task msgid: %" PRId64
               " execute take time: %d us",
               message_->session_id, message_->header.msg_id, take_time);

    common::SocketSessionImpl session;
    session.Send(message_, resp);

    message_ = nullptr;
    response_sent_ = true;
}

} // namespace watch
}
}
