#include "range.h"

#include "server/range_server.h"
#include "monitor/statistics.h"
#include "watch/watch_util.h"

namespace sharkstore {
namespace dataserver {
namespace range {


bool Range::verifyWatchKey(const watchpb::WatchKeyValue& kv, const metapb::RangeEpoch& epoch, errorpb::Error *&err) {
    if (kv.key().empty()) {
        err = new errorpb::Error;
        err->set_message("insufficient watch keys size");
        return false;
    }

    auto db_key = store_->EncodeWatchKey(kv);
    if (!KeyInRange(db_key)) {
        if (!EpochIsEqual(epoch)) {
            err = StaleEpochError(epoch);
        } else {
            err = KeyNotInRange(db_key);
        }
        return false;
    }

    return true;
}

Status Range::GetAndResp( watch::WatcherPtr pWatcher, const watchpb::WatchCreateRequest& req, const std::string &dbKey, const bool &prefix,
                          int64_t &version, watchpb::DsWatchResponse *dsResp) {
    version = 0;
    Status ret;
    if(prefix) {
        std::string dbKeyEnd;
        dbKeyEnd.assign(dbKey);
        if (!watch::NextComparableBytes(dbKey.data(), dbKey.length(), dbKeyEnd)) {
            FLOG_ERROR("GetAndResp:NextComparableBytes error.");
            return Status(Status::kUnknown);
        }

        auto watcherServer = context_->WatchServer();
        auto ws = watcherServer->GetWatcherSet(pWatcher->GetFirstKey());

        auto result = ws->loadFromDb(store_.get(), watchpb::PUT, dbKey, dbKeyEnd, version, meta_.GetTableID(), dsResp);
        if (result.first <= 0) {
            delete dsResp;
            dsResp = nullptr;
        }

    } else {
        auto resp = dsResp->mutable_resp();
        resp->set_watchid(pWatcher->GetWatcherId());
        resp->set_code(static_cast<int>(ret.code()));
        auto evt = resp->add_events();

        std::string dbValue;

        ret = store_->Get(dbKey, &dbValue);
        if (ret.ok()) {

            evt->set_type(watchpb::PUT);

            int64_t db_version(0);
            std::string value;
            std::string ext;
            if(!watch::DecodeValue(dbValue, &db_version, &value, &ext)) {
                return Status(Status::kCorruption, "decode watch value", EncodeToHexString(dbValue));
            }

            auto userKv = new watchpb::WatchKeyValue;
            for(const auto& key: req.kv().key()) {
                userKv->add_key(key);
            }
            userKv->set_value(value);
            userKv->set_version(db_version);
            userKv->set_tableid(meta_.GetTableID());

            version = db_version;

            RANGE_LOG_INFO("GetAndResp ok, db_version: [%" PRIu64 "]", db_version);
        } else {
            evt->set_type(watchpb::DELETE);
            RANGE_LOG_INFO("GetAndResp code_: %s  key:%s",
                           ret.ToString().c_str(), EncodeToHexString(dbKey).c_str());
        }
    }
    return ret;
}


void Range::WatchGet(common::ProtoMessage *msg, watchpb::DsWatchRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->Statistics()->PushTime(monitor::HistogramType::kQWait, btime - msg->begin_time);

    auto ds_resp = new watchpb::DsWatchResponse;
    auto header = ds_resp->mutable_header();
    std::string db_key;
    std::string db_value;

    auto prefix = req.req().prefix();
    const auto& watch_kv = req.req().kv();

    RANGE_LOG_DEBUG("WatchGet begin  msgid: %" PRId64 " session_id: %" PRId64, msg->header.msg_id, msg->session_id);

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        watch::EncodeKey(&db_key, meta_.GetTableID(), watch_kv);
        bool in_range = KeyInRange(db_key);
        auto epoch = req.header().range_epoch();
        bool is_equal = EpochIsEqual(epoch);
        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(db_key);
                break;
            }
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("WatchGet error: %s", err->message().c_str());
        common::SetResponseHeader(req.header(), header, err);
        context_->SocketSession()->Send(msg, ds_resp);
        return;
    }

    //add watch if client version is not equal to ds side
    auto client_version = req.req().startversion();

    // to do add watch
    std::vector<std::string> keys;
    for (const auto& key: watch_kv.key()) {
        keys.push_back(key);
    }
    auto watch_type = watch::WatchType::kKey;
    if(prefix) {
        watch_type = watch::WatchType::kPrefix;
    }
    int64_t expireTime = (req.req().longpull() > 0) ? get_micro_second() + req.req().longpull()*1000 : msg->expire_time*1000;
    auto w_ptr = std::make_shared<watch::Watcher>(meta_.GetTableID(), std::move(keys), client_version, expireTime, msg, watch_type);

    watch::WatchCode wcode;
    auto watch_server = context_->WatchServer();
    if(prefix) {
        auto hashKey = w_ptr->GetKeys();
        std::string encode_key;
        watch::EncodeKey(&encode_key, w_ptr->GetTableId(), {w_ptr->GetFirstKey()});

        std::vector<watch::CEventBufferValue> vecUpdKeys;

        auto retPair = eventBuffer->loadFromBuffer(encode_key, client_version, vecUpdKeys);
        int32_t memCnt(retPair.first);
        auto verScope = retPair.second;
        RANGE_LOG_DEBUG("loadFromBuffer key:%s hit count[%" PRId32 "] version scope:%" PRId32 "---%" PRId32 " client_version:%" PRId64 ,
                        EncodeToHexString(encode_key).c_str(), memCnt, verScope.first, verScope.second, client_version);

        if(memCnt > 0) {
            auto resp = ds_resp->mutable_resp();
            resp->set_code(Status::kOk);
            resp->set_scope(watchpb::RESPONSE_PART);

            for (auto j = 0; j < memCnt; j++) {
                auto evt = resp->add_events();

                for (decltype(vecUpdKeys[j].key().size()) k = 0; k < vecUpdKeys[j].key().size(); k++) {
                    evt->mutable_kv()->add_key(vecUpdKeys[j].key(k));
                }
                evt->mutable_kv()->set_value(vecUpdKeys[j].value());
                evt->mutable_kv()->set_version(vecUpdKeys[j].version());
                evt->set_type(vecUpdKeys[j].type());
            }

            w_ptr->Send(ds_resp);
            return;
        } else {
            w_ptr->setBufferFlag(memCnt);
            wcode = watch_server->AddPrefixWatcher(w_ptr, store_.get());
        }
    } else {
        wcode = watch_server->AddKeyWatcher(w_ptr, store_.get());
    }

    if(watch::WatchCode::kOK == wcode) {
        return;
    } else if(watch::WatchCode::kWatcherNotNeed == wcode) {
        auto btime = get_micro_second();
        //to do get from db again
        GetAndResp(w_ptr, req.req(), db_key, prefix, dbVersion, ds_resp);
        context_->Statistics()->PushTime(monitor::HistogramType::kQWait,
                                       get_micro_second() - btime);
        w_ptr->Send(ds_resp);
    } else {
        RANGE_LOG_ERROR("add watcher exception(%d).", static_cast<int>(wcode));
        return;
    }
}

void Range::PureGet(common::ProtoMessage *msg, watchpb::DsKvWatchGetMultiRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->Statistics()->PushTime(monitor::HistogramType::kQWait, btime - msg->begin_time);

    auto ds_resp = new watchpb::DsKvWatchGetMultiResponse;
    auto header = ds_resp->mutable_header();
    //encode key and value
    std::string dbKey;
    std::string dbKeyEnd;
    std::string dbValue;
    //int64_t version{0};
    int64_t minVersion(0);
    int64_t maxVersion(0);
    auto prefix = req.prefix();

    RANGE_LOG_DEBUG("PureGet beginmsgid: %" PRId64 " session_id: %" PRId64, msg->header.msg_id, msg->session_id);

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.kv().key();
        if (key.empty()) {
            RANGE_LOG_WARN("PureGet error: key empty");
            err = KeyNotInRange("EmptyKey");
            break;
        }

        //encode key
        watch::EncodeKV(meta_.GetTableID(), req.kv(), &dbKey, &dbValue);
        RANGE_LOG_INFO("PureGet key before:%s after:%s", key[0].c_str(), EncodeToHexString(dbKey).c_str());

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(dbKey);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(dbKey);
                break;
            }
        }

        auto resp = ds_resp;
        auto btime = get_micro_second();
        storage::Iterator *it = nullptr;
        Status::Code code = Status::kOk;

        if (prefix) {
            dbKeyEnd.assign(dbKey);
            if(!watch::NextComparableBytes(dbKey.data(), dbKey.length(), dbKeyEnd)) {
                //to do set error message
                break;
            }
            RANGE_LOG_DEBUG("PureGet key scope %s---%s", EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbKeyEnd).c_str());

            //need to encode and decode
            std::shared_ptr<storage::Iterator> iterator(store_->NewIterator(dbKey, dbKeyEnd));
            uint32_t count{0};

            for (int i = 0; iterator->Valid() ; ++i) {
                count++;
                auto kv = resp->add_kvs();
                auto tmpDbKey = iterator.get()->key();
                auto tmpDbValue = iterator.get()->value();

                auto s = watch::DecodeKV(tmpDbKey, tmpDbValue, kv);
                if (!s.ok()) {
                    // TODO:
                    continue;
                }
                //to judge version after decoding value and spliting version from value
                if (minVersion > kv->version()) {
                    minVersion = kv->version();
                }
                if(maxVersion < kv->version()) {
                    maxVersion = kv->version();
                }

                iterator->Next();
            }

            RANGE_LOG_DEBUG("PureGet ok:%d ", count);
            code = Status::kOk;
        } else {

            auto ret = store_->Get(dbKey, &dbValue);
            if(ret.ok()) {
                //to do decode value version
                RANGE_LOG_DEBUG("PureGet: dbKey:%s dbValue:%s  ", EncodeToHexString(dbKey).c_str(),
                           EncodeToHexString(dbValue).c_str());

                auto kv = resp->add_kvs();
                if (Status::kOk != WatchEncodeAndDecode::DecodeKv(funcpb::kFuncPureGet, meta_.GetTableID(), kv, dbKey, dbValue, err)) {
                    RANGE_LOG_WARN("DecodeKv fail. dbvalue:%s  err:%s", EncodeToHexString(dbValue).c_str(),
                               err->message().c_str());
                }
            }

            RANGE_LOG_DEBUG("PureGet code:%d msg:%s ", ret.code(), ret.ToString().data());
            code = ret.code();
        }
        context_->Statistics()->PushTime(monitor::HistogramType::kQWait, get_micro_second() - btime);

        resp->set_code(static_cast<int32_t>(code));
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("PureGet error: %s", err->message().c_str());
    }

    common::SetResponseHeader(req.header(), header, err);
    context_->SocketSession()->Send(msg, ds_resp);
}

void Range::WatchPut(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req) {
    errorpb::Error *err = nullptr;
    std::string dbKey;

    auto btime = get_micro_second();
    context_->Statistics()->PushTime(monitor::HistogramType::kQWait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("WatchPut begin msgid: %" PRId64 " session_id: %" PRId64, msg->header.msg_id, msg->session_id);

    if (!CheckWriteable()) {
        auto resp = new watchpb::DsKvWatchPutResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        const auto& kv = req.req().kv();
        if (!verifyWatchKey(kv, req.header().range_epoch(), err)) {
            break;
        }

        RANGE_LOG_DEBUG("WatchPut key:%s value:%s", kv.key(0).c_str(), kv.value().c_str());

        if (!WatchPutSubmit(msg, req)) {
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("WatchPut error: %s", err->message().c_str());
        auto resp = new watchpb::DsKvWatchPutResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

void Range::WatchDel(common::ProtoMessage *msg, watchpb::DsKvWatchDeleteRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->Statistics()->PushTime(monitor::HistogramType::kQWait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("WatchDel begin, msgid: %" PRId64 " session_id: %" PRId64, msg->header.msg_id, msg->session_id);

    if (!CheckWriteable()) {
        auto resp = new watchpb::DsKvWatchDeleteResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        if (!verifyWatchKey(req.req().kv(), req.header().range_epoch(), err)) {
            break;
        }

        if (!WatchDeleteSubmit(msg, req)) {
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("WatchDel error: %s", err->message().c_str());

        auto resp = new watchpb::DsKvWatchDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

bool Range::WatchPutSubmit(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req) {
    auto &kv = req.req().kv();

    if (is_leader_ && kv.key_size() > 0 ) {
        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvWatchPut);
            cmd.set_allocated_kv_watch_put_req(req.release_req());
        });
        return ret.ok();
    }
    return false;
}

bool Range::WatchDeleteSubmit(common::ProtoMessage *msg,
                            watchpb::DsKvWatchDeleteRequest &req) {
    auto &kv = req.req().kv();

    if (is_leader_ && kv.key_size() > 0 ) {
        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvWatchDel);
            cmd.set_allocated_kv_watch_del_req(req.release_req());
        });
        return ret.ok();
    }
    return false;
}

Status Range::ApplyWatchPut(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    Status ret;
    errorpb::Error *err = nullptr;
    auto btime = get_micro_second();
    auto version = static_cast<int64_t>(raft_index);
    const auto& kv = cmd.kv_watch_put_req().kv();

    RANGE_LOG_DEBUG("ApplyWatchPut new version[%" PRIu64 "]", version);

    do {
        ret = store_->WatchPut(kv, version);
        context_->Statistics()->PushTime(monitor::HistogramType::kStore, get_micro_second() - btime);
        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyWatchPut failed: kv=%s, err=%s", kv.ShortDebugString().c_str(), ret.ToString().c_str());
            if (ret.code() == Status::kNotInRange) {
                err = KeyNotInRange(store_->EncodeWatchKey(kv));
            }
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = static_cast<uint64_t>(kv.ByteSizeLong());
            CheckSplit(len);

            // TODO: notify
            std::string errMsg;
            int32_t retCnt = WatchNotify(watchpb::PUT, kv, version, errMsg);
            if (retCnt < 0) {
                FLOG_ERROR("WatchNotify-put failed, ret:%d, msg:%s", retCnt, errMsg.c_str());
            } else {
                FLOG_DEBUG("WatchNotify-put success, count:%d, msg:%s", retCnt, errMsg.c_str());
            }
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new watchpb::DsKvWatchPutResponse;
        resp->mutable_resp()->set_code(ret.code());
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

Status Range::ApplyWatchDel(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    Status ret;
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    const auto &req = cmd.kv_watch_del_req();
    int64_t version = static_cast<int64_t>(raft_index);

    std::vector<watchpb::WatchKeyValue> deleted_keys;
    do {
        // delete from store
        ret = store_->WatchDelete(req.kv(), req.prefix(), &deleted_keys);
        context_->Statistics()->PushTime(monitor::HistogramType::kStore, get_micro_second() - btime);
        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyWatchDel failed: kv=%s, err=%s", req.kv().ShortDebugString().c_str(), ret.ToString().c_str());

            if (ret.code() == Status::kNotInRange) {
                err = KeyNotInRange(store_->EncodeWatchKey(req.kv()));
            }
            break;
        }

        // notify
        if (cmd.cmd_id().node_id() == node_id_) {
            for (const auto& kv: deleted_keys) {
                // TODO:
                int32_t retCnt(0);
                std::string errMsg("");
                retCnt = WatchNotify(watchpb::DELETE, kv, version, errMsg, req.prefix());
                if (retCnt < 0) {
                    FLOG_ERROR("WatchNotify-del failed, ret:%d, msg:%s", retCnt, errMsg.c_str());
                } else {
                    FLOG_DEBUG("WatchNotify-del success, watch_count:%d, msg:%s", retCnt, errMsg.c_str());
                }
            }
        }
    } while (false);

    if(cmd.cmd_id().node_id() == node_id_) {
        auto resp = new watchpb::DsKvWatchDeleteResponse;
        resp->mutable_resp()->set_code(ret.code());
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

int32_t Range::WatchNotify(const watchpb::EventType evtType, const watchpb::WatchKeyValue& kv, const int64_t &version, std::string &errMsg, bool prefix) {

    if(kv.key_size() == 0) {
        errMsg.assign("WatchNotify--key is empty.");
        return -1;
    }

    std::vector<watch::WatcherPtr> vecNotifyWatcher;
    std::vector<watch::WatcherPtr> vecPrefixNotifyWatcher;

    //continue to get prefix key
    std::vector<std::string> decodeKeys;
    std::string hashKey;
    std::string dbKey;

    bool hasPrefix(prefix);
    if(!hasPrefix && kv.key_size() > 1) {
        hasPrefix = true;
    }

    for(const auto& key : kv.key()) {
        decodeKeys.push_back(key);
        //only push the first key
        break;
    }

    watch::EncodeKey(&hashKey, meta_.GetTableID(), decodeKeys);
    if(hasPrefix) {
        int16_t tmpCnt{0};
        for(const auto& key : kv.key()) {
            ++tmpCnt;
            if(tmpCnt == 1) continue;
            //to do skip the first element
            decodeKeys.push_back(key);
        }
        watch::EncodeKey(&dbKey, meta_.GetTableID(), decodeKeys);
    } else {
        dbKey = hashKey;
    }

    FLOG_DEBUG("WatchNotify haskkey:%s  key:%s version:%" PRId64, EncodeToHexString(hashKey).c_str(), EncodeToHexString(dbKey).c_str(), version);
    if(hasPrefix) {
        auto value = std::make_shared<watch::CEventBufferValue>(kv, evtType, version);

        if (!eventBuffer->enQueue(hashKey, value.get())) {
            FLOG_ERROR("load delete event kv to buffer error.");
        }
    }

    auto dbValue = kv.value();
    int64_t currDbVersion{version};
    auto watch_server = context_->WatchServer();

    auto watch_set = watch_server->GetWatcherSet(kv.key(0));
    watch_set->GetKeyWatchers(evtType, vecNotifyWatcher, dbKey, currDbVersion);

    //start to send user kv to client
    int32_t watchCnt = vecNotifyWatcher.size();
    FLOG_DEBUG("single key notify:%" PRId32 " key:%s", watchCnt, EncodeToHexString(dbKey).c_str());
    for(auto i = 0; i < watchCnt; i++) {
        auto dsResp = new watchpb::DsWatchResponse;
        auto resp = dsResp->mutable_resp();
        auto evt = resp->add_events();
        evt->set_allocated_kv(new  watchpb::WatchKeyValue(kv));
        evt->mutable_kv()->set_version(currDbVersion);
        evt->set_type(evtType);

        SendNotify(vecNotifyWatcher[i], dsResp);
    }

    if(hasPrefix) {
        watch_set->GetPrefixWatchers(evtType, vecPrefixNotifyWatcher, hashKey, currDbVersion);

        watchCnt = vecPrefixNotifyWatcher.size();
        FLOG_DEBUG("prefix key notify:%" PRId32 " key:%s", watchCnt, EncodeToHexString(dbKey).c_str());

        for( auto i = 0; i < watchCnt; i++) {

            int64_t startVersion(vecPrefixNotifyWatcher[i]->getKeyVersion());
            auto dsResp = new watchpb::DsWatchResponse;

            std::vector<watch::CEventBufferValue> vecUpdKeys;
            vecUpdKeys.clear();

            auto retPair = eventBuffer->loadFromBuffer(hashKey, startVersion, vecUpdKeys);

            int32_t memCnt(retPair.first);
            auto verScope = retPair.second;
            RANGE_LOG_DEBUG("loadFromBuffer key:%s hit count[%" PRId32 "] version scope:%" PRId32 "---%" PRId32 " client_version:%" PRId64 ,
                            EncodeToHexString(hashKey).c_str(), memCnt, verScope.first, verScope.second, startVersion);

            if (0 == memCnt) {
                FLOG_ERROR("doudbt no changing, notify %d/%"
                                   PRId32
                                   " key:%s", i+1, watchCnt, EncodeToHexString(dbKey).c_str());

                delete dsResp;
                dsResp = nullptr;

            } else if (memCnt > 0) {
                FLOG_DEBUG("notify %d/%"
                                   PRId32
                                   " loadFromBuffer key:%s  hit count:%"
                                   PRId32, i+1, watchCnt, EncodeToHexString(dbKey).c_str(), memCnt);


                auto resp = dsResp->mutable_resp();
                resp->set_code(Status::kOk);
                resp->set_scope(watchpb::RESPONSE_PART);

                for (auto j = 0; j < memCnt; j++) {
                    auto evt = resp->add_events();

                    for (decltype(vecUpdKeys[j].key().size()) k = 0; k < vecUpdKeys[j].key().size(); k++) {
                        evt->mutable_kv()->add_key(vecUpdKeys[j].key(k));
                    }
                    evt->mutable_kv()->set_value(vecUpdKeys[j].value());
                    evt->mutable_kv()->set_version(vecUpdKeys[j].version());
                    evt->set_type(vecUpdKeys[j].type());

                }

            } else {

                //get all from db
                FLOG_INFO("overlimit version in memory,get from db now. notify %d/%"
                                  PRId32
                                  " key:%s version:%"
                                  PRId64,
                          i+1, watchCnt, EncodeToHexString(dbKey).c_str(), startVersion);
                //use iterator
                std::string dbKeyEnd{""};
                dbKeyEnd.assign(dbKey);
                if( 0 != watch::NextComparableBytes(dbKey.data(), dbKey.length(), dbKeyEnd)) {
                    //to do set error message
                    FLOG_ERROR("NextComparableBytes error.");
                    return -1;
                }
                //RANGE_LOG_DEBUG("WatchNotify key scope %s---%s", EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbKeyEnd).c_str());
                auto watcherServer = context_->WatchServer();
                auto ws = watcherServer->GetWatcherSet(kv.key(0));

                auto result = ws->loadFromDb(store_.get(), evtType, dbKey, dbKeyEnd, startVersion, meta_.GetTableID(), dsResp);
                if(result.first <= 0) {
                    delete dsResp;
                    dsResp = nullptr;
                }

                //scopeFlag = 1;
                FLOG_DEBUG("notify %d/%" PRId32 " load from db, db-count:%" PRId32 " key:%s ", i+1, watchCnt, result.first, EncodeToHexString(dbKey).c_str());

            }

            if (hasPrefix && watchCnt > 0 && dsResp != nullptr) {
                SendNotify(vecPrefixNotifyWatcher[i], dsResp, true);
            }

        }
    }

    return watchCnt;
}

int32_t Range::SendNotify(watch::WatcherPtr& w, watchpb::DsWatchResponse *ds_resp, bool prefix) {
    auto watch_server = context_->WatchServer();
    auto resp = ds_resp->mutable_resp();
    auto w_id = w->GetWatcherId();

    resp->set_watchid(w_id);

    w->Send(ds_resp);

    //delete watch
    watch::WatchCode del_ret = watch::WatchCode::kOK;
    if (!prefix && w->GetType() == watch::WatchType::kKey) {
        del_ret = watch_server->DelKeyWatcher(w);
        if (del_ret == watch::WatchCode::kOK) {
            RANGE_LOG_WARN(" DelKeyWatcher error, watch_id[%" PRId64 "]", w_id);
        } else {
            RANGE_LOG_WARN(" DelKeyWatcher execute end. watch_id:%" PRIu64, w_id);
        }
    }

    if (prefix && w->GetType() == watch::WatchType::kKey) {
        del_ret = watch_server->DelPrefixWatcher(w);
        if (del_ret == watch::WatchCode::kOK) {
            RANGE_LOG_WARN(" DelPrefixWatcher error, watch_id[%" PRId64 "]", w_id);
        } else {
            RANGE_LOG_WARN(" DelPrefixWatcher execute end. watch_id:%" PRIu64, w_id);
        }
    }
    return static_cast<int32_t>(del_ret);
}


}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
