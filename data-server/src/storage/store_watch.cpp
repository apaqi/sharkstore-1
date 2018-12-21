#include "store.h"

#include "base/util.h"
#include "common/ds_encoding.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

std::string Store::encodeWatchKey(const watchpb::WatchKeyValue& kv) const {
    std::string buf;
    buf.push_back(kStoreKVPrefixByte);
    EncodeUint64Ascending(&buf, table_id_);
    for (const auto& k : kv.key()) {
        EncodeBytesAscending(&buf, k.c_str(), k.size());
    }
    return buf;
}

std::string Store::encodeWatchValue(const watchpb::WatchKeyValue& kv, int64_t version) const {
    std::string buf;
    EncodeIntValue(&buf, 2, version);
    EncodeBytesValue(&buf, 3, kv.value().c_str(), kv.value().size());
    EncodeBytesValue(&buf, 3, kv.ext().c_str(), kv.ext().size());
    return buf;
}

bool Store::decodeWatchKey(const std::string& key, watchpb::WatchKeyValue *kv) const {
    for (size_t offset = kRowPrefixLength; offset < key.length(); ) {
        if (!DecodeBytesAscending(key, offset, kv->add_key())) {
            return false;
        }
    }
    return true;
}

bool Store::decodeWatchValue(const std::string& value, watchpb::WatchKeyValue *kv) const {
    size_t offset = 0;
    int64_t version = 0;
    if (!DecodeIntValue(value, offset, &version)) return false;
    kv->set_version(version);
    if (!DecodeBytesValue(value, offset, kv->mutable_value())) return false;
    return DecodeBytesValue(value, offset, kv->mutable_ext());
}

std::string Store::EncodeWatchKey(const watchpb::WatchKeyValue& kv) const {
    return encodeWatchKey(kv);
}

Status Store::WatchPut(const watchpb::WatchKeyValue& kv, int64_t version) {
    auto key = encodeWatchKey(kv);
    if (!checkInRange(key)) {
        return Status(Status::kNotInRange);
    }
    auto value = encodeWatchValue(kv, version);
    return this->Put(key, value);
}

Status Store::watchDeletePrefix(const watchpb::WatchKeyValue& kv,
                         std::vector<watchpb::WatchKeyValue> *deleted_keys) {
    auto start = encodeWatchKey(kv);
    auto limit = NextComparable(start);
    assert(!limit.empty());
    std::unique_ptr<Iterator> iter(NewIterator(start ,limit));

    rocksdb::WriteBatch batch;
    rocksdb::Status s;
    uint64_t keys_written = 0;
    uint64_t bytes_written = 0;
    while (iter->Valid()) {
        // add to deleted
        auto db_key = iter->key();
        watchpb::WatchKeyValue deleted_kv;
        if (!decodeWatchKey(db_key , &deleted_kv)) {
            return Status(Status::kCorruption, "decode watch key", EncodeToHex(db_key));
        }
        deleted_keys->push_back(std::move(deleted_kv));
        // add to batch
        s = batch.Delete(db_key);
        if (!s.ok()) {
            return Status(Status::kIOError, "watch batch add delete", s.ToString());
        }

        bytes_written += db_key.size();
        ++keys_written;
        iter->Next();
    }
    if (!iter->status().ok()) {
        return iter->status();
    }

    s = db_->Write(write_options_, &batch);
    if (!s.ok()) {
        return Status(Status::kIOError, "watch batch write", s.ToString());
    } else {
        addMetricWrite(keys_written, bytes_written);
        return Status::OK();
    }
}

Status Store::WatchDelete(const watchpb::WatchKeyValue& key, bool prefix,
                   std::vector<watchpb::WatchKeyValue> *deleted_keys) {
    assert(deleted_keys != nullptr);
    if (prefix) {
        return watchDeletePrefix(key, deleted_keys);
    }

    auto db_key = encodeWatchKey(key);
    if (!checkInRange(db_key)) {
        return Status(Status::kNotInRange);
    }
    deleted_keys->push_back(key);
    return this->Delete(db_key);
}

Status Store::watchGetPrefix(const watchpb::WatchKeyValue& kv,
                      std::vector<watchpb::WatchKeyValue> *result) {
    auto start = encodeWatchKey(kv);
    auto limit = NextComparable(start);
    assert(!limit.empty());
    std::unique_ptr<Iterator> iter(NewIterator(start ,limit));
    uint64_t bytes_read = 0;
    uint64_t keys_read = 0;
    while (iter->Valid()) {
        auto db_key = iter->key();
        auto db_value = iter->value();
        result->push_back(watchpb::WatchKeyValue());
        if (!decodeWatchKey(db_key, &result->back())) {
            return Status(Status::kCorruption, "decode watch key", EncodeToHex(db_key));
        }
        if (!decodeWatchValue(db_value, &result->back())) {
            return Status(Status::kCorruption, "decode watch value", EncodeToHex(db_value));
        }

        bytes_read += db_key.size();
        bytes_read += db_value.size();
        ++keys_read;
        iter->Next();
    }
    addMetricRead(keys_read, bytes_read);
    return iter->status();
}

Status Store::WatchGet(const watchpb::WatchKeyValue& key, bool prefix,
                std::vector<watchpb::WatchKeyValue> *result) {
    assert(result != nullptr);

    if (prefix) {
        return watchGetPrefix(key, result);
    }

    std::string db_value;
    auto db_key = encodeWatchKey(key);
    if (!checkInRange(db_key)) {
        return Status(Status::kNotInRange);
    }
    auto s = this->Get(db_key, &db_value);
    if (!s.ok()) {
        return s;
    }
    watchpb::WatchKeyValue result_kv = key; // copy key
    if (!decodeWatchValue(db_value, &result_kv)) { // decode value
        return Status(Status::kCorruption, "decode watch value", EncodeToHex(db_value));
    }
    result->push_back(std::move(result_kv)); // add to result
    return Status::OK();
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
