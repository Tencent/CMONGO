/* connpool.cpp
*/

/*    Copyright 2009 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

// _ todo: reconnect?

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include "mongo/client/connpool.h"

#include <limits>
#include <string>

#include "mongo/db/client.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/global_conn_pool.h"
#include "mongo/client/replica_set_monitor.h"
#include "mongo/client/syncclusterconnection.h"
#include "mongo/executor/connection_pool_stats.h"
#include "mongo/stdx/chrono.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"

namespace mongo {

namespace {
const int kDefaultIdleTimeout = std::numeric_limits<int>::max();
const int kDefaultMaxInUse = std::numeric_limits<int>::max();
const int kMaxTimeoutSeconds = 20;
}  // namespace

using std::endl;
using std::list;
using std::map;
using std::set;
using std::string;
using std::vector;

// ------ PoolForHost ------

PoolForHost::PoolForHost()
    : _socketTimeoutSecs(0.0),
      _created(0),
      _minValidCreationTimeMicroSec(0),
      _type(ConnectionString::INVALID),
      _maxPoolSize(kPoolSizeUnlimited),
      _checkedOut(0),
      _badConns(0),
      _waitingConns(0),
      _parentDestroyed(false),
      _inShutdown(false),
      _overloadConfig()
{}

PoolForHost::PoolForHost(const PoolForHost& other)
    : _socketTimeoutSecs(other._socketTimeoutSecs),
      _created(other._created),
      _minValidCreationTimeMicroSec(other._minValidCreationTimeMicroSec),
      _type(other._type),
      _maxPoolSize(other._maxPoolSize),
      _checkedOut(other._checkedOut),
      _badConns(other._badConns),
      _waitingConns(0),
      _parentDestroyed(other._parentDestroyed),
      _overloadConfig(other._overloadConfig) {
    verify(_created == 0);
    verify(other._pool.size() == 0);
    _inShutdown.store(other._inShutdown.load());
}

PoolForHost::~PoolForHost() {
    clear();
}

void PoolForHost::clear() {
    if (!_parentDestroyed) {
        log() << "Dropping all pooled connections to " << _hostName << "(with timeout of "
              << _socketTimeoutSecs << " seconds)";
    }

    while (!_pool.empty()) {
        StoredConnection sc = _pool.top();
        delete sc.conn;
        _pool.pop();
    }
}

void PoolForHost::done(DBConnectionPool* pool, DBClientBase* c) {
    bool isFailed = c->isFailed();

    --_checkedOut;
    _checkedOutMap.erase(c->getConnectionId());

    // Remember that this host had a broken connection for later
    if (isFailed) {
        _badConns++;
        reportBadConnectionAt(c->getSockCreationMicroSec());
    }

    if (isFailed ||
        // Another (later) connection was reported as broken to this host
        (c->getSockCreationMicroSec() < _minValidCreationTimeMicroSec) ||
        // We have a pool size that we need to enforce
        (_maxPoolSize >= 0 && static_cast<int>(_pool.size()) >= _maxPoolSize)) {
		LOG(0) << "conn from pool of " << _hostName << " on destroy;"
			   << "isFailed:" << isFailed
			   << ",laterBroken:" << (c->getSockCreationMicroSec() < _minValidCreationTimeMicroSec);
        pool->onDestroy(c);
        delete c;
    } else {
        // The connection is probably fine, save for later
        _pool.push(c);
    }
}

void PoolForHost::reportBadConnectionAt(uint64_t microSec) {
    if (microSec != DBClientBase::INVALID_SOCK_CREATION_TIME &&
        microSec > _minValidCreationTimeMicroSec) {
        _minValidCreationTimeMicroSec = microSec;
        log() << "Detected bad connection created at " << _minValidCreationTimeMicroSec
              << " microSec, clearing pool for " << _hostName << " of " << openConnections()
              << " connections" << endl;
        clear();
    }
}

bool PoolForHost::isBadSocketCreationTime(uint64_t microSec) {
    return microSec != DBClientBase::INVALID_SOCK_CREATION_TIME &&
        microSec <= _minValidCreationTimeMicroSec;
}

DBClientBase* PoolForHost::get(DBConnectionPool* pool, double socketTimeout) {
    while (!_pool.empty()) {
        StoredConnection sc = _pool.top();
        _pool.pop();

        if (!sc.ok()) {
            _badConns++;
            pool->onDestroy(sc.conn);
            delete sc.conn;
            continue;
        }

        verify(sc.conn->getSoTimeout() == socketTimeout);

        ++_checkedOut;
        _checkedOutMap[sc.conn->getConnectionId()] = Date_t::now();
        return sc.conn;
    }

    return NULL;
}

void PoolForHost::flush() {
    while (!_pool.empty()) {
        StoredConnection c = _pool.top();
        _pool.pop();
        delete c.conn;
    }
}

void PoolForHost::getStaleConnections(Date_t idleThreshold, vector<DBClientBase*>& stale) {
    vector<StoredConnection> all;
    while (!_pool.empty()) {
        StoredConnection c = _pool.top();
        _pool.pop();

        if (c.ok() && !c.addedBefore(idleThreshold))
            all.push_back(c);
        else {
            _badConns++;
            stale.push_back(c.conn);
        }
    }

    for (size_t i = 0; i < all.size(); i++) {
        _pool.push(all[i]);
    }
}


PoolForHost::StoredConnection::StoredConnection(DBClientBase* c)
    : conn(std::move(c)), added(Date_t::now()) {}

bool PoolForHost::StoredConnection::ok() {
    // Poke the connection to see if we're still ok
    return conn->isStillConnected();
}

bool PoolForHost::StoredConnection::addedBefore(Date_t time) {
    return added < time;
}

void PoolForHost::createdOne(DBClientBase* base) {
    if (_created == 0)
        _type = base->type();
    ++_created;
    // _checkedOut is used to indicate the number of in-use connections so
    // though we didn't actually check this connection out, we bump it here.
    ++_checkedOut;
    _checkedOutMap[base->getConnectionId()] = Date_t::now();
}

void PoolForHost::initializeHostName(const std::string& hostName) {
    if (_hostName.empty()) {
        _hostName = hostName;
    }
}

void PoolForHost::waitForFreeConnection(int timeout, stdx::unique_lock<stdx::mutex>& lk) {
    auto condition = [&] { return (!_overloadConfig.isInUsePoolOverloaded(numInUse()) || _inShutdown.load()); };

    if (_overloadConfig.isQueueOverloaded(_waitingConns)) {
        log() << "Connection pool: over loaded, current size: " << _waitingConns;
        uassert(ErrorCodes::ServerOverloaded,
                str::stream() << "too many waiting connections to " << _hostName << ":" << timeout,
                false);
    }

    _waitingConns ++;

    LOG(2) << "wait for connection count " << _waitingConns << " in-use " << numInUse();

    // Reset timeout
    if (timeout <= 0 || timeout > kMaxTimeoutSeconds) {
        timeout = kMaxTimeoutSeconds;
    }

    stdx::chrono::seconds timeoutSeconds{1};
    int currentWaittime = 0;
    while (currentWaittime < timeout) {
        // Check remote connection before getting the local connection
        // Wait local connection for 1s and then check remote connection again
        if (cc().getCurrent()->isStillConnected()) {
            // Remote still connected
            if (_cv.wait_for(lk, timeoutSeconds, condition)) {
                break;
            } else {
                currentWaittime ++;
                continue;
            }
        } else {
            // Remote connection has gone
            _waitingConns --;
            uassert(ErrorCodes::ExceededTimeLimit,
                    str::stream() << "remote client closed", false);
        }
    }

    if (currentWaittime >= timeout) {
        // Failed to get a connection in given timeout
        _waitingConns --;
        uassert(ErrorCodes::ExceededTimeLimit,
                str::stream() << "too many connections to " << _hostName << ":" << timeout,
                false);
    }

    // Got a connection
    _waitingConns --;
    LOG(2) << "end wait for connection count " << _waitingConns << " in-use " << numInUse();
}

void PoolForHost::notifyWaiters() {
    _cv.notify_one();
}

void PoolForHost::shutdown() {
    _inShutdown.store(true);
    _cv.notify_all();
}

int PoolForHost::numSlowQueries() const {
    int count = 0;
    auto currentTimestamp = Date_t::now();
    for (CheckedOutMap::const_iterator i = _checkedOutMap.begin(); i != _checkedOutMap.end(); i++) {
        auto startTimestamp = i->second;
        if (_overloadConfig.isOverloadSlowQuery(currentTimestamp - startTimestamp)) {
            count ++;
        }
    }
    return count;
}

// ------ DBConnectionPool::Detail ------

class DBConnectionPool::Detail {
public:
    template <typename Connect>
    static DBClientBase* get(DBConnectionPool* _this,
                             const std::string& host,
                             double timeout,
                             Connect connect) {
        while (!(_this->_inShutdown.load())) {
            // Get a connection from the pool, if there is one.
            std::unique_ptr<DBClientBase> c(_this->_get(host, timeout));
            if (c) {
                // This call may throw.
                _this->onHandedOut(c.get());
                return c.release();
            }

            // If there are no pooled connections for this host, create a new connection. If
            // there are too many connections in this pool to make a new one, block until a
            // connection is released.
            {
                stdx::unique_lock<stdx::mutex> lk(_this->_mutex);
                PoolForHost& p = _this->_pools[PoolKey(host, timeout)];
                if (_this->_overloadConfig.isInUsePoolOverloaded(p.numInUse())) {
                    LOG(2) << "Too many in-use connections " << p.numInUse() << "; waiting until there are fewer than "
                           << _this->_overloadConfig.getInUsePoolSize();
                    p.waitForFreeConnection(timeout, lk);
                } else {
                    // Drop the lock here, so we can connect without holding it.
                    // _finishCreate will take the lock again.
                    lk.unlock();

                    // Create a new connection and return. All Connect functions
                    // should throw if they cannot create a connection.
                    auto c = connect();
                    invariant(c);
                    return _this->_finishCreate(host, timeout, c);
                }
            }
        }

        // If we get here, we are in shutdown, and it does not matter what we return.
        invariant(_this->_inShutdown.load());
        uassert(ErrorCodes::ShutdownInProgress, "connection pool is in shutdown", false);
        MONGO_UNREACHABLE;
    }
};

// ------ DBConnectionPool ------

const int PoolForHost::kPoolSizeUnlimited(std::numeric_limits<int>::max());

DBConnectionPool::DBConnectionPool()
    : _name("dbconnectionpool"),
      _maxPoolSize(PoolForHost::kPoolSizeUnlimited),
      _idleTimeout(kDefaultIdleTimeout),
      _inShutdown(false),
      _hooks(new list<DBConnectionHook*>()),
      _overloadConfig()
{}

void DBConnectionPool::shutdown() {
    if (!_inShutdown.swap(true)) {
        stdx::lock_guard<stdx::mutex> L(_mutex);
        for (auto i = _pools.begin(); i != _pools.end(); i++) {
            PoolForHost& p = i->second;
            p.shutdown();
        }
    }
}

DBClientBase* DBConnectionPool::_get(const string& ident, double socketTimeout) {
    uassert(17382, "Can't use connection pool during shutdown", !inShutdown());
    stdx::lock_guard<stdx::mutex> L(_mutex);
    PoolForHost& p = _pools[PoolKey(ident, socketTimeout)];
    p.setMaxPoolSize(_maxPoolSize);
    p.setOverloadConfig(_overloadConfig);
    p.setSocketTimeout(socketTimeout);
    p.initializeHostName(ident);
    return p.get(this, socketTimeout);
}

size_t DBConnectionPool::getCheckedOutSize(const string& ident, double socketTimeout) const {
    stdx::lock_guard<stdx::mutex> L(_mutex);
	auto p = _pools.find(PoolKey(ident, socketTimeout));
	if (p == _pools.end()) {
		return 0;
	}
	return p->second.numInUse();
}

int DBConnectionPool::openConnections(const string& ident, double socketTimeout) {
    stdx::lock_guard<stdx::mutex> L(_mutex);
    PoolForHost& p = _pools[PoolKey(ident, socketTimeout)];
    return p.openConnections();
}

bool DBConnectionPool::checkSlowQueries() {
    if (_overloadConfig.isOverloadEnabled()) {
        stdx::lock_guard<stdx::mutex> L(_mutex);
        uint64_t slowCount = 0;
        uint64_t totalCount = 0;
        for (PoolMap::iterator i = _pools.begin(); i != _pools.end(); i++) {
            PoolForHost& p = i->second;
            totalCount += p.numInUse();
            slowCount += p.numSlowQueries();
        }
        int oldPoolSize = _overloadConfig.getOverloadInUsePoolSize();
        _overloadConfig.overloadResizing(_overloadConfig.checkOverloadSQ(slowCount, totalCount));
        if (oldPoolSize != _overloadConfig.getOverloadInUsePoolSize()) {
            LOG(0) << _name << " in-use pool resize from " << oldPoolSize
                   << " to " << _overloadConfig.getOverloadInUsePoolSize();
        }
    }
    return false;
}

void DBConnectionPool::checkOverloadExpired() {
    auto currentTimestamp = Date_t::now();
    if (_overloadConfig.isOverloadExpired(currentTimestamp)) {
        LOG(2) << _name << " disable overload checking automatically after one hour";
        _overloadConfig.disableOverload();
    }
}

DBClientBase* DBConnectionPool::_finishCreate(const string& ident,
                                              double socketTimeout,
                                              DBClientBase* conn) {
    {
        stdx::lock_guard<stdx::mutex> L(_mutex);
        PoolForHost& p = _pools[PoolKey(ident, socketTimeout)];
        p.setMaxPoolSize(_maxPoolSize);
        p.setOverloadConfig(_overloadConfig);
        p.initializeHostName(ident);
        p.createdOne(conn);
    }

    try {
        onCreate(conn);
        onHandedOut(conn);
    } catch (std::exception&) {
        delete conn;
        throw;
    }

    log() << "Successfully connected to " << ident << " (" << openConnections(ident, socketTimeout)
          << " connections now open to " << ident << " with a " << socketTimeout
          << " second timeout)";

    return conn;
}

DBClientBase* DBConnectionPool::get(const ConnectionString& url, double socketTimeout) {
    auto connect = [&]() {
        string errmsg;
        auto c = url.connect(errmsg, socketTimeout);
        uassert(13328, _name + ": connect failed " + url.toString() + " : " + errmsg, c);
        return c;
    };

    return Detail::get(this, url.toString(), socketTimeout, connect);
}

DBClientBase* DBConnectionPool::get(const string& host, double socketTimeout) {
    auto connect = [&] {
        const ConnectionString cs(uassertStatusOK(ConnectionString::parse(host)));

        string errmsg;
        auto c = cs.connect(errmsg, socketTimeout);
        if (!c) {
            throw SocketException(SocketException::CONNECT_ERROR,
                                  host,
                                  11002,
                                  str::stream() << _name << " error: " << errmsg);
        }
        return c;
    };

    return Detail::get(this, host, socketTimeout, connect);
}

int DBConnectionPool::getNumBadConns(const string& host, double socketTimeout) const {
    stdx::lock_guard<stdx::mutex> L(_mutex);
    auto it = _pools.find(PoolKey(host, socketTimeout));
    return (it == _pools.end()) ? 0 : it->second.getNumBadConns();
}

void DBConnectionPool::onRelease(DBClientBase* conn) {
    if (_hooks->empty()) {
        return;
    }

    for (list<DBConnectionHook*>::iterator i = _hooks->begin(); i != _hooks->end(); i++) {
        (*i)->onRelease(conn);
    }
}

void DBConnectionPool::release(const string& host, DBClientBase* c) {
    onRelease(c);

    stdx::unique_lock<stdx::mutex> lk(_mutex);
    PoolForHost& p = _pools[PoolKey(host, c->getSoTimeout())];
    p.done(this, c);

    lk.unlock();
    p.notifyWaiters();
}


DBConnectionPool::~DBConnectionPool() {
    // connection closing is handled by ~PoolForHost
    // Do not log in destruction, because global connection pools get
    // destroyed after the logging framework.
    stdx::lock_guard<stdx::mutex> L(_mutex);
    for (PoolMap::iterator i = _pools.begin(); i != _pools.end(); i++) {
        PoolForHost& p = i->second;
        p._parentDestroyed = true;
    }
}

void DBConnectionPool::flush() {
    stdx::lock_guard<stdx::mutex> L(_mutex);
    for (PoolMap::iterator i = _pools.begin(); i != _pools.end(); i++) {
        PoolForHost& p = i->second;
        p.flush();
    }
}

void DBConnectionPool::clear() {
    stdx::lock_guard<stdx::mutex> L(_mutex);
    LOG(2) << "Removing connections on all pools owned by " << _name << endl;
    for (PoolMap::iterator iter = _pools.begin(); iter != _pools.end(); ++iter) {
        iter->second.clear();
    }
}

void DBConnectionPool::removeHost(const string& host) {
    stdx::lock_guard<stdx::mutex> L(_mutex);
    LOG(2) << "Removing connections from all pools for host: " << host << endl;
    for (PoolMap::iterator i = _pools.begin(); i != _pools.end(); ++i) {
        const string& poolHost = i->first.ident;
        if (!serverNameCompare()(host, poolHost) && !serverNameCompare()(poolHost, host)) {
            // hosts are the same
            i->second.clear();
        }
    }
}

void DBConnectionPool::addHook(DBConnectionHook* hook) {
    _hooks->push_back(hook);
}

void DBConnectionPool::onCreate(DBClientBase* conn) {
    if (_hooks->size() == 0)
        return;

    for (list<DBConnectionHook*>::iterator i = _hooks->begin(); i != _hooks->end(); i++) {
        (*i)->onCreate(conn);
    }
}

void DBConnectionPool::onHandedOut(DBClientBase* conn) {
    if (_hooks->size() == 0)
        return;

    for (list<DBConnectionHook*>::iterator i = _hooks->begin(); i != _hooks->end(); i++) {
        (*i)->onHandedOut(conn);
    }
}

void DBConnectionPool::onDestroy(DBClientBase* conn) {
    if (_hooks->size() == 0)
        return;

    for (list<DBConnectionHook*>::iterator i = _hooks->begin(); i != _hooks->end(); i++) {
        (*i)->onDestroy(conn);
    }
}

void DBConnectionPool::appendConnectionStats(executor::ConnectionPoolStats* stats) const {
    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        for (PoolMap::const_iterator i = _pools.begin(); i != _pools.end(); ++i) {
            if (i->second.numCreated() == 0)
                continue;

            // Mongos may use either a replica set uri or a list of addresses as
            // the identifier here, so we always take the first server parsed out
            // as our label for connPoolStats. Note that these stats will collide
            // with any existing stats for the chosen host.
            auto uri = ConnectionString::parse(i->first.ident);
            invariant(uri.isOK());
            HostAndPort host = uri.getValue().getServers().front();

            executor::ConnectionStatsPerHost hostStats{
                static_cast<size_t>(i->second.numInUse()),
                static_cast<size_t>(i->second.numAvailable()),
                static_cast<size_t>(i->second.numCreated())};
            stats->updateStatsForHost(host, hostStats);
        }
    }
}

bool DBConnectionPool::serverNameCompare::operator()(const string& a, const string& b) const {
    const char* ap = a.c_str();
    const char* bp = b.c_str();

    while (true) {
        if (*ap == '\0' || *ap == '/') {
            if (*bp == '\0' || *bp == '/')
                return false;  // equal strings
            else
                return true;  // a is shorter
        }

        if (*bp == '\0' || *bp == '/')
            return false;  // b is shorter

        if (*ap < *bp)
            return true;
        else if (*ap > *bp)
            return false;

        ++ap;
        ++bp;
    }
    verify(false);
}

bool DBConnectionPool::poolKeyCompare::operator()(const PoolKey& a, const PoolKey& b) const {
    if (DBConnectionPool::serverNameCompare()(a.ident, b.ident))
        return true;

    if (DBConnectionPool::serverNameCompare()(b.ident, a.ident))
        return false;

    return a.timeout < b.timeout;
}

bool DBConnectionPool::isConnectionGood(const string& hostName, DBClientBase* conn) {
    if (conn == NULL) {
        return false;
    }

    if (conn->isFailed()) {
        return false;
    }

    {
        stdx::lock_guard<stdx::mutex> sl(_mutex);
        PoolForHost& pool = _pools[PoolKey(hostName, conn->getSoTimeout())];
        if (pool.isBadSocketCreationTime(conn->getSockCreationMicroSec())) {
            return false;
        }
    }

    return true;
}

void DBConnectionPool::taskDoWork() {
    vector<DBClientBase*> toDelete;

    auto idleThreshold = Date_t::now() - _idleTimeout;
    {
        // we need to get the connections inside the lock
        // but we can actually delete them outside
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        for (PoolMap::iterator i = _pools.begin(); i != _pools.end(); ++i) {
            i->second.getStaleConnections(idleThreshold, toDelete);
        }
    }

    for (size_t i = 0; i < toDelete.size(); i++) {
        try {
            onDestroy(toDelete[i]);
            delete toDelete[i];
        } catch (...) {
            // we don't care if there was a socket error
        }
    }
}

// ------ ScopedDbConnection ------

ScopedDbConnection::ScopedDbConnection(const std::string& host, double socketTimeout)
    : _host(host),
      _conn(globalConnPool.get(host, socketTimeout)),
      _socketTimeoutSecs(socketTimeout) {
    _setSocketTimeout();
}

ScopedDbConnection::ScopedDbConnection(const ConnectionString& host, double socketTimeout)
    : _host(host.toString()),
      _conn(globalConnPool.get(host, socketTimeout)),
      _socketTimeoutSecs(socketTimeout) {
    _setSocketTimeout();
}

void ScopedDbConnection::done() {
    if (!_conn) {
        return;
    }

    globalConnPool.release(_host, _conn);
    _conn = NULL;
}

void ScopedDbConnection::_setSocketTimeout() {
    if (!_conn)
        return;
    if (_conn->type() == ConnectionString::MASTER)
        ((DBClientConnection*)_conn)->setSoTimeout(_socketTimeoutSecs);
    else if (_conn->type() == ConnectionString::SYNC)
        ((SyncClusterConnection*)_conn)->setAllSoTimeouts(_socketTimeoutSecs);
}

ScopedDbConnection::~ScopedDbConnection() {
    if (_conn) {
        if (_conn->isFailed()) {
            if (_conn->getSockCreationMicroSec() == DBClientBase::INVALID_SOCK_CREATION_TIME) {
                kill();
            } else {
                // The pool takes care of deleting the failed connection - this
                // will also trigger disposal of older connections in the pool
                done();
            }
        } else {
            /* see done() comments above for why we log this line */
            log() << "scoped connection to " << _conn->getServerAddress()
                  << " not being returned to the pool" << endl;
            kill();
        }
    }
}

void ScopedDbConnection::clearPool() {
    globalConnPool.clear();
}

AtomicInt32 AScopedConnection::_numConnections;

}  // namespace mongo
