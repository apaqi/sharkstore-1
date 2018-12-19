#include "watch_util.h"

#include "common/ds_encoding.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

void EncodeKey(std::string* buf, uint64_t tableId, const std::vector<std::string>& keys) {
    assert(buf != nullptr);
    buf->push_back(static_cast<char>(1)); // store prefix byte
    EncodeUint64Ascending(buf, tableId); // column 1
    assert(buf->length() == 9);
    for (const auto& key: keys) {
        EncodeBytesAscending(buf, key.c_str(), key.length());
    }
}

void EncodeKey(std::string* buf, uint64_t tableId, const watchpb::WatchKeyValue& kv) {
    assert(buf != nullptr);
    buf->push_back(static_cast<char>(1)); // store prefix byte
    EncodeUint64Ascending(buf, tableId); // column 1
    assert(buf->length() == 9);
    for (const auto& key: kv.key()) {
        EncodeBytesAscending(buf, key.c_str(), key.length());
    }
}

bool DecodeKey(const std::string& buf, std::vector<std::string>* keys) {
    assert(buf.length() > 9);
    size_t offset = 0;
    for (offset = 9; offset < buf.length();) {
        std::string key;
        if (!DecodeBytesAscending(buf, offset, &key)) {
            return false;
        }
        keys->push_back(std::move(key));
    }
    return true;
}

void EncodeValue(std::string* buf, int64_t version, const std::string& value, const std::string& extend) {
    assert(buf != nullptr);
    EncodeIntValue(buf, 2, version);
    EncodeBytesValue(buf, 3, value.c_str(), value.length());
    EncodeBytesValue(buf, 4, extend.c_str(), extend.length());
}

bool DecodeValue(const std::string& buf, int64_t* version, std::string* value, std::string* extend) {
    assert(version != nullptr && value != nullptr && extend != nullptr);
    size_t offset = 0;
    if (!DecodeIntValue(buf, offset, version)) return false;
    if (!DecodeBytesValue(buf, offset, value)) return false;
    return DecodeBytesValue(buf, offset, extend);
}

void EncodeKV(uint64_t tableID, const watchpb::WatchKeyValue &kv, std::string *db_key, std::string *db_value) {
    assert(db_key != nullptr);
    assert(db_value != nullptr);

    // encode key
    std::vector<std::string> keys;
    for (auto i = 0; i < kv.key_size(); i++) {
        keys.push_back(kv.key(i);
    }
    EncodeKey(db_key, tableID, keys);

    // encode value
    EncodeValue(db_value, kv.version(), kv.value(), kv.ext();
}

Status DecodeKV(const std::string& db_key, const std::string& db_value, watchpb::WatchKeyValue *kv) {
    if (db_key.empty() || db_value.empty()) {
        return Status(Status::kInvalidArgument, "watch decode kv", "key or value is empty");
    }

    // decode keys
    std::vector<std::string> keys;
    if(!DecodeKey(db_key, &keys)) {
        return Status(Status::kCorruption, "decode watch key", EncodeToHexString(db_key));
    }
    kv->clear_key();
    for (const auto& key: keys) {
        kv->add_key(key);
    }

    // decode value
    std::string val, ext;
    int64_t version = 0;
    if(!DecodeValue(db_value, &version, &val, &ext)) {
        return Status(Status::kCorruption, "decode watch value", EncodeToHexString(db_value));
    }
    kv->set_value(std::move(val));
    kv->set_version(version);
    kv->set_ext(std::move(ext));

    return Status::OK();
}

static bool GetHashKey(watch::WatcherPtr pWatcher, bool prefix, const int64_t &tableId, std::string *encodeKey) {

    auto hashKeys = pWatcher->GetKeys(prefix);
    watch::Watcher::EncodeKey(encodeKey, tableId, hashKeys);

    return true;
}

bool NextComparableBytes(const char *key, const int32_t &len, std::string &result) {
    if(len > 0) {
        result.resize(static_cast<size_t>(len));
    }
    for(auto i = len - 1; i >= 0; i--) {
        auto key_num = static_cast<uint8_t>(key[i]);
        if(key_num < 0xff) {
            key_num ++;
            result[i] = static_cast<char>(key_num);
            return true;
        }
    }
    return false;
}



} // namespace watch
} // namespace dataserver
} // namespace sharkstore

