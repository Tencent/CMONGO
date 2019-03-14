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
#pragma once

#include <cstdint>

#include "mongo/util/time_support.h"

namespace mongo {

/**
 * Holds for connection configuration for overload
 */
class ConnectionPoolOverloadRuntime {
public:
    ConnectionPoolOverloadRuntime();
    ConnectionPoolOverloadRuntime(const ConnectionPoolOverloadRuntime &runtime);
    ~ConnectionPoolOverloadRuntime() {}

    ConnectionPoolOverloadRuntime &operator=(const ConnectionPoolOverloadRuntime &runtime);

    bool isOverloadEnabled() const {
        return _enableOverload;
    }

    int getNormalQueueSize() const {
        return _normalQueueSize;
    }

    int getOverloadQueueSize() const {
        return _overloadQueueSize;
    }

    int getNormalInUsePoolSize() const {
        return _normalInUsePoolSize;
    }

    int getOverloadInUsePoolSize() const {
        return _overloadInUsePoolSize;
    }

    int getInUsePoolSize() const {
        return _enableOverload ? _overloadInUsePoolSize : _normalInUsePoolSize;
    }

    bool isInUsePoolOverloaded(int inUseSize) const {
        return (_enableOverload && _overloadInUsePoolSize > 0 ?
                inUseSize >= _overloadInUsePoolSize :
                (_normalInUsePoolSize > 0 && inUseSize >= _normalInUsePoolSize));
    }

    bool isQueueOverloaded(int queueSize) const {
        return (_enableOverload && _overloadQueueSize > 0 ?
                queueSize >= _overloadQueueSize :
                (_normalQueueSize > 0 && queueSize >= _normalQueueSize));
    }

    int isOverloadSlowQuery(Milliseconds queryTime) const {
        return queryTime >= Milliseconds(_overloadSQThreshold);
    }

protected:
    /**
     * If overload is enabled, once the connection request could not be
     * satisfied, overloaded error will be returned
     */
    bool _enableOverload;
    int _normalQueueSize;
    int _overloadQueueSize;
    /**
     * The maximum number of processing connections for a host.  This includes pending
     * connections in setup/refresh. It's designed to rate limit connection storms rather than
     * steady state processing (as maxConnections does).
     */
    int _normalInUsePoolSize;
    int _overloadInUsePoolSize;
    int _overloadSQThreshold;
};

class ConnectionPoolOverloadConfig : public ConnectionPoolOverloadRuntime {
public:
    ConnectionPoolOverloadConfig();
    ~ConnectionPoolOverloadConfig() {}

    bool isOverloadExpired(Date_t timestamp) const;

    void overloadResizing(int flag);

    int checkOverloadSQ(uint64_t slowCount, uint64_t totalCount) const;

    void setEnableOverload(bool enable) {
        if (enable) {
            enableOverload();
        } else {
            disableOverload();
        }
    }

    void enableOverload() {
        _enableOverload = true;
        _enableOverloadTimestamp = Date_t::now();
    }

    void disableOverload() {
        _enableOverload = false;
    }

    bool isOverloadResizing() const {
        return _enableOverloadResizing;
    }

    void setOverloadResizing(bool enable) {
        if (enable) {
            enableOverloadResizing();
        } else {
            disableOverloadResizing();
        }
    }

    void enableOverloadResizing() {
        _enableOverloadResizing = true;
    }

    void disableOverloadResizing() {
        _enableOverloadResizing = false;
    }

    void setNormalQueueSize(int queueSize) {
        _normalQueueSize = queueSize;
        if (queueSize < _overloadQueueSize) {
            _overloadQueueSize = queueSize;
        }
    }

    void setOverloadQueueSize(int queueSize) {
        if (queueSize > _normalQueueSize && _normalQueueSize > 0) {
            queueSize = _normalQueueSize;
        }
        _overloadQueueSize = queueSize;
    }

    void setNormalInUsePoolSize(int poolSize) {
        _normalInUsePoolSize = poolSize;
        setOverloadInUsePoolSize(poolSize);
    }

    void setOverloadInUsePoolSize(int poolSize) {
        if (poolSize <= 0 || poolSize > _normalInUsePoolSize) {
            poolSize = _normalInUsePoolSize;
        }
        _overloadMaxInUsePoolSize = poolSize;
        _overloadMinInUsePoolSize = poolSize / 2;
        _overloadInUsePoolSize = poolSize;
    }

    int getOverloadMaxInUsePoolSize() const {
        return _overloadMaxInUsePoolSize;
    }

protected:
    Date_t _enableOverloadTimestamp;
    bool _enableOverloadResizing;
    int _overloadMinInUsePoolSize;
    int _overloadMaxInUsePoolSize;
};

}
