/**
 *    Tencent is pleased to support the open source community by making CMONGO available.
 *
 *    Copyright (C) 2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *    Licensed under the GNU Affero General Public License Version 3 (the "License");
 *    you may not use this file except in compliance with the License. You may obtain a 
 *    copy of the License at https://www.gnu.org/licenses/agpl-3.0.en.html
 *
 *    Unless required by applicable law or agreed to in writing, software distributed under
 *    the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *    either express or implied. See the License for the specific language governing permissions
 *    and limitations under the License.
*/
#include "mongo/util/overload_config.h"

#include <limits>

namespace mongo {

namespace {
const int kDefaultMaxQueueSize = 100;
const int kDefaultMaxInUse = std::numeric_limits<int>::max();
const int kDefaultOverloadQueueSize = 50;
const int kDefaultOverloadSQThreshold = 100;
const int kOverloadExpired = 3600;
const int kOverloadSQHighWM = 80;
const int kOverloadSQLowWM = 20;
}   // namespace

ConnectionPoolOverloadRuntime::ConnectionPoolOverloadRuntime()
: _enableOverload(false),
  _normalQueueSize(kDefaultMaxQueueSize),
  _overloadQueueSize(kDefaultOverloadQueueSize),
  _normalInUsePoolSize(kDefaultMaxInUse),
  _overloadInUsePoolSize(kDefaultMaxInUse),
  _overloadSQThreshold(kDefaultOverloadSQThreshold) {
}

ConnectionPoolOverloadRuntime::ConnectionPoolOverloadRuntime(const ConnectionPoolOverloadRuntime &runtime)
: _enableOverload(runtime._enableOverload),
  _normalQueueSize(runtime._normalQueueSize),
  _overloadQueueSize(runtime._overloadQueueSize),
  _normalInUsePoolSize(runtime._normalInUsePoolSize),
  _overloadInUsePoolSize(runtime._overloadInUsePoolSize),
  _overloadSQThreshold(runtime._overloadSQThreshold) {
}

ConnectionPoolOverloadRuntime &ConnectionPoolOverloadRuntime::operator=(const ConnectionPoolOverloadRuntime &runtime) {
    _enableOverload = runtime._enableOverload;
    _normalQueueSize = runtime._normalQueueSize;
    _overloadQueueSize = runtime._overloadQueueSize;
    _normalInUsePoolSize = runtime._normalInUsePoolSize;
    _overloadInUsePoolSize = runtime._overloadInUsePoolSize;
    _overloadSQThreshold = runtime._overloadSQThreshold;
    return *this;
}

ConnectionPoolOverloadConfig::ConnectionPoolOverloadConfig()
: ConnectionPoolOverloadRuntime(),
  _enableOverloadResizing(true),
  _overloadMinInUsePoolSize(kDefaultMaxInUse),
  _overloadMaxInUsePoolSize(kDefaultMaxInUse) {
}

bool ConnectionPoolOverloadConfig::isOverloadExpired(Date_t timestamp) const {
    if (_enableOverload) {
        if (timestamp - _enableOverloadTimestamp > Seconds(kOverloadExpired)) {
            return true;
        }
    }
    return false;
}

void ConnectionPoolOverloadConfig::overloadResizing(int flag) {
    if (_enableOverload && _enableOverloadResizing && flag != 0) {
        int step = (_overloadMaxInUsePoolSize - _overloadMinInUsePoolSize) / 10;
        if (flag > 0) {
            _overloadInUsePoolSize = std::max(_overloadInUsePoolSize - step, _overloadMinInUsePoolSize);
        } else {
            _overloadInUsePoolSize = std::min(_overloadInUsePoolSize + step, _overloadMaxInUsePoolSize);
        }
    }
}

int ConnectionPoolOverloadConfig::checkOverloadSQ(uint64_t slowCount, uint64_t totalCount) const {
    if (totalCount == 0) {
        return -1;
    }
    uint64_t rate = slowCount * 100 / totalCount;
    return (rate > kOverloadSQHighWM ? 1 : (rate < kOverloadSQLowWM ? -1 : 0));
}
}
