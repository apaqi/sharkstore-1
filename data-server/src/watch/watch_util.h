_Pragma("once");

#include <stdint.h>
#include <string>

#include "base/status.h"
#include "proto/gen/watchpb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

void EncodeKey(std::string* buf, uint64_t tableId, const std::vector<std::string>& keys);
void EncodeKey(std::string* buf, uint64_t tableId, const watchpb::WatchKeyValue& kv);
bool DecodeKey(const std::string& buf, std::vector<std::string>* keys);

bool DecodeValue(const std::string& buf, int64_t* version, std::string* value, std::string* extend);
void EncodeValue(std::string* buf, int64_t version, const std::string& value, const std::string& extend);

void EncodeKV(uint64_t tableID, const watchpb::WatchKeyValue &kv, std::string *db_key, std::string *db_value);
Status DecodeKV(const std::string& db_key, const std::string& db_value, watchpb::WatchKeyValue *kv);

bool NextComparableBytes(const char *key, const int32_t &len, std::string &result);

} // namespace watch
} // namespace dataserver
} // namespace sharkstore
