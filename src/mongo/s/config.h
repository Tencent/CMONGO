/**
 *    Copyright (C) 2008-2015 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <set>

#include "mongo/db/jsobj.h"
#include "mongo/db/repl/optime.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/s/client/shard.h"
#include "mongo/util/concurrency/mutex.h"
#include "mongo/grpc/masterproto/master.pb.h"
#include "mongo/db/namespace_string.h"

namespace mongo {

class ChunkManager;
class CollectionType;
class DatabaseType;
class DBConfig;
class OperationContext;

class CollectionInfo {
public: 
    virtual ~CollectionInfo();
	virtual bool isSharded() const = 0;
    virtual std::shared_ptr<ChunkManager> getCM() const = 0;
    virtual void resetCM(ChunkManager* cm) = 0;
	virtual void unshard() = 0;
	virtual bool isDirty() const = 0;
	virtual bool wasDropped() const = 0;
	virtual void save(OperationContext *txn, const std::string& ns) = 0;
    virtual void useChunkManager(std::shared_ptr<ChunkManager> manager) = 0;
    virtual bool unique() const = 0;
    virtual BSONObj key() const = 0;
    virtual repl::OpTime getConfigOpTime() const = 0;
};

class CollectionInfoDefault :public CollectionInfo {
public:
    CollectionInfoDefault();

    CollectionInfoDefault(OperationContext* txn, const CollectionType& in, repl::OpTime);
    ~CollectionInfoDefault();

    bool isSharded() const final {
        return _cm.get();
    }

    std::shared_ptr<ChunkManager> getCM() const final {
        return _cm;
    }

    void resetCM(ChunkManager* cm) final;

    void unshard() final;

    bool isDirty() const final {
        return _dirty;
    }

    bool wasDropped() const final {
        return _dropped;
    }

    void save(OperationContext* txn, const std::string& ns) final;

    void useChunkManager(std::shared_ptr<ChunkManager> manager) final;

    bool unique() const final {
        return _unique;
    }

    BSONObj key() const final {
        return _key;
    }

    repl::OpTime getConfigOpTime() const final {
        return _configOpTime;
    }

private:
    BSONObj _key;
    bool _unique;
    std::shared_ptr<ChunkManager> _cm;
    bool _dirty;
    bool _dropped;
    repl::OpTime _configOpTime;
};

class CollectionInfoCMongo :public CollectionInfo {
public:
	CollectionInfoCMongo();

	CollectionInfoCMongo(OperationContext* txn, const CollectionType& in, repl::OpTime);

    ~CollectionInfoCMongo();

	bool isSharded() const final {
		return _cm.get();
	}

    std::shared_ptr<ChunkManager> getCM() const final {
        return _cm;
    }

    void resetCM(ChunkManager* cm) final;

    void unshard() final;

    bool isDirty() const final {
		return false;
    }

    bool wasDropped() const final {
		return false;
    }

    void save(OperationContext* txn, const std::string& ns) final;

    void useChunkManager(std::shared_ptr<ChunkManager> manager) final;

    bool unique() const final;

    BSONObj key() const final {
        return _key;
    }

	const NamespaceString& ns() const;

	const std::string routeMd5() const {
		return _routeMd5;
	}

    repl::OpTime getConfigOpTime() const final {
        return _configOpTime;
    }

	Status parseFromPB(const masterproto::TableRoutes& pb);

private:
    BSONObj _key;
    std::shared_ptr<ChunkManager> _cm;
    repl::OpTime _configOpTime;
	NamespaceString _ns;
	std::string _routeMd5;
};

class DBConfig {
public:
	DBConfig();
	virtual ~DBConfig();
    virtual const std::string& name() const = 0;

    /**
     * Whether sharding is enabled for this database.
     */
    virtual bool isShardingEnabled() const = 0;

    virtual const ShardId& getPrimaryId() const = 0;

    /**
     * Removes all cached metadata for the specified namespace so that subsequent attempts to
     * retrieve it will cause a full reload.
     */
    virtual void invalidateNs(const std::string& ns) = 0;

    virtual void enableSharding(OperationContext* txn) = 0;

    /**
       @return true if there was sharding info to remove
     */
    virtual bool removeSharding(OperationContext* txn, const std::string& ns) = 0;

    /**
     * @return whether or not the 'ns' collection is partitioned
     */
    virtual bool isSharded(const std::string& ns) = 0;

    // Atomically returns *either* the chunk manager *or* the primary shard for the collection,
    // neither if the collection doesn't exist.
    virtual void getChunkManagerOrPrimary(OperationContext* txn,
                                  const std::string& ns,
                                  std::shared_ptr<ChunkManager>& manager,
                                  std::shared_ptr<Shard>& primary) = 0;

    virtual std::shared_ptr<ChunkManager> getChunkManager(OperationContext* txn,
                                                  const std::string& ns,
                                                  bool reload = false,
                                                  bool forceReload = false) = 0;
    virtual std::shared_ptr<ChunkManager> getChunkManagerIfExists(OperationContext* txn,
                                                          const std::string& ns,
                                                          bool reload = false,
                                                          bool forceReload = false) = 0;

    /**
     * Returns shard id for primary shard for the database for which this DBConfig represents.
     */
    virtual const ShardId& getShardId(OperationContext* txn, const std::string& ns) = 0;

    virtual void setPrimary(OperationContext* txn, const ShardId& newPrimaryId) = 0;

    /**
     * Returns true if it is successful at loading the DBConfig, false if the database is not found,
     * and throws on all other errors.
     */
    virtual bool load(OperationContext* txn) = 0;
    virtual bool reload(OperationContext* txn) = 0;

    virtual bool dropDatabase(OperationContext*, std::string& errmsg) = 0;

    virtual void getAllShardIds(std::set<ShardId>* shardIds) = 0;
    virtual void getAllShardedCollections(std::set<std::string>& namespaces) = 0;

};

/**
 * top level configuration for a database
 */
class DBConfigDefault :public DBConfig {
public:
    DBConfigDefault(std::string name, const DatabaseType& dbt, repl::OpTime configOpTime);
    ~DBConfigDefault();

    /**
     * The name of the database which this entry caches.
     */
    const std::string& name() const final {
        return _name;
    }

    /**
     * Whether sharding is enabled for this database.
     */
    bool isShardingEnabled() const final {
        return _shardingEnabled;
    }

    const ShardId& getPrimaryId() const final {
        return _primaryId;
    }

    /**
     * Removes all cached metadata for the specified namespace so that subsequent attempts to
     * retrieve it will cause a full reload.
     */
    void invalidateNs(const std::string& ns) final;

    void enableSharding(OperationContext* txn) final;

    /**
       @return true if there was sharding info to remove
     */
    bool removeSharding(OperationContext* txn, const std::string& ns) final;

    /**
     * @return whether or not the 'ns' collection is partitioned
     */
    bool isSharded(const std::string& ns) final;

    // Atomically returns *either* the chunk manager *or* the primary shard for the collection,
    // neither if the collection doesn't exist.
    void getChunkManagerOrPrimary(OperationContext* txn,
                                  const std::string& ns,
                                  std::shared_ptr<ChunkManager>& manager,
                                  std::shared_ptr<Shard>& primary) final;

    std::shared_ptr<ChunkManager> getChunkManager(OperationContext* txn,
                                                  const std::string& ns,
                                                  bool reload = false,
                                                  bool forceReload = false) final;
    std::shared_ptr<ChunkManager> getChunkManagerIfExists(OperationContext* txn,
                                                          const std::string& ns,
                                                          bool reload = false,
                                                          bool forceReload = false) final;

    /**
     * Returns shard id for primary shard for the database for which this DBConfig represents.
     */
    const ShardId& getShardId(OperationContext* txn, const std::string& ns) final;

    void setPrimary(OperationContext* txn, const ShardId& newPrimaryId) final;

    /**
     * Returns true if it is successful at loading the DBConfig, false if the database is not found,
     * and throws on all other errors.
     */
    bool load(OperationContext* txn) final;
    bool reload(OperationContext* txn) final;

    bool dropDatabase(OperationContext*, std::string& errmsg) final;

    void getAllShardIds(std::set<ShardId>* shardIds) final;
    void getAllShardedCollections(std::set<std::string>& namespaces) final;

protected:
    typedef std::map<std::string, CollectionInfoDefault> CollectionInfoMap;
    typedef AtomicUInt64::WordType Counter;

    bool _dropShardedCollections(OperationContext* txn,
                                 int& num,
                                 std::set<ShardId>& shardIds,
                                 std::string& errmsg);

    /**
     * Returns true if it is successful at loading the DBConfig, false if the database is not found,
     * and throws on all other errors.
     * Also returns true without reloading if reloadIteration is not equal to the _reloadCount.
     * This is to avoid multiple threads attempting to reload do duplicate work.
     */
    bool _loadIfNeeded(OperationContext* txn, Counter reloadIteration);

    void _save(OperationContext* txn, bool db = true, bool coll = true);

    // All member variables are labeled with one of the following codes indicating the
    // synchronization rules for accessing them.
    //
    // (L) Must hold _lock for access.
    // (I) Immutable, can access freely.
    // (S) Self synchronizing, no explicit locking needed.
    //
    // Mutex lock order:
    // _hitConfigServerLock -> _lock
    //

    // Name of the database which this entry caches
    const std::string _name;  // (I)

    // Primary shard id
    ShardId _primaryId;  // (L) TODO: SERVER-22175 enforce this

    // Whether sharding has been enabled for this database
    bool _shardingEnabled;  // (L) TODO: SERVER-22175 enforce this

    // Set of collections and lock to protect access
    stdx::mutex _lock;
    CollectionInfoMap _collections;  // (L)

    // OpTime of config server when the database definition was loaded.
    repl::OpTime _configOpTime;  // (L)

    // Ensures that only one thread at a time loads collection configuration data from
    // the config server
    stdx::mutex _hitConfigServerLock;

    // Increments every time this performs a full reload. Since a full reload can take a very
    // long time for very large clusters, this can be used to minimize duplicate work when multiple
    // threads tries to perform full reload at roughly the same time.
    AtomicUInt64 _reloadCount;  // (S)
};

class DBConfigCMongo :public DBConfig {
public:
    DBConfigCMongo(const std::string& name, 
			const std::string& primaryShard, bool shardingEnabled);
    ~DBConfigCMongo();

    /**
     * The name of the database which this entry caches.
     */
    const std::string& name() const final {
        return _name;
    }

    /**
     * Whether sharding is enabled for this database.
     */
    bool isShardingEnabled() const final {
        return _shardingEnabled;
    }

    const ShardId& getPrimaryId() const final {
        return _primaryId;
    }

    /**
     * Removes all cached metadata for the specified namespace so that subsequent attempts to
     * retrieve it will cause a full reload.
     */
    void invalidateNs(const std::string& ns) final;

    void enableSharding(OperationContext* txn) final;

    /**
       @return true if there was sharding info to remove
     */
    bool removeSharding(OperationContext* txn, const std::string& ns) final;

    /**
     * @return whether or not the 'ns' collection is partitioned
     */
    bool isSharded(const std::string& ns) final;

    // Atomically returns *either* the chunk manager *or* the primary shard for the collection,
    // neither if the collection doesn't exist.
    void getChunkManagerOrPrimary(OperationContext* txn,
                                  const std::string& ns,
                                  std::shared_ptr<ChunkManager>& manager,
                                  std::shared_ptr<Shard>& primary) final;

    std::shared_ptr<ChunkManager> getChunkManager(OperationContext* txn,
                                                  const std::string& ns,
                                                  bool reload = false,
                                                  bool forceReload = false) final;
    std::shared_ptr<ChunkManager> getChunkManagerIfExists(OperationContext* txn,
                                                          const std::string& ns,
                                                          bool reload = false,
                                                          bool forceReload = false) final;

    /**
     * Returns shard id for primary shard for the database for which this DBConfig represents.
     */
    const ShardId& getShardId(OperationContext* txn, const std::string& ns) final;

    void setPrimary(OperationContext* txn, const ShardId& newPrimaryId) final;

    /**
     * Returns true if it is successful at loading the DBConfig, false if the database is not found,
     * and throws on all other errors.
     */
    bool load(OperationContext* txn) final;
    bool reload(OperationContext* txn) final;

    bool dropDatabase(OperationContext*, std::string& errmsg) final;

    void getAllShardIds(std::set<ShardId>* shardIds) final;
    void getAllShardedCollections(std::set<std::string>& namespaces) final;

	Status parseTableFromPB(const masterproto::TableRoutes& pb);

	Status removeTable(const std::string& tableName);

private:
    typedef std::map<std::string, std::unique_ptr<CollectionInfoCMongo>> CollectionInfoMap;

    // Name of the database which this entry caches
    const std::string _name;  // (I)

    // Primary shard id
    ShardId _primaryId;  // (L) TODO: SERVER-22175 enforce this

    // Whether sharding has been enabled for this database
    bool _shardingEnabled;  // (L) TODO: SERVER-22175 enforce this

    // Set of collections and lock to protect access
    stdx::mutex _lock;
    CollectionInfoMap _collections;  // (L)

    // OpTime of config server when the database definition was loaded.
    repl::OpTime _configOpTime;  // (L)
};

class ConfigServer {
public:
    static void reloadSettings(OperationContext* txn);

    /**
     * For use in mongos and mongod which needs notifications about changes to shard and config
     * server replset membership to update the ShardRegistry.
     *
     * This is expected to be run in an existing thread.
     */
    static void replicaSetChangeShardRegistryUpdateHook(const std::string& setName,
                                                        const std::string& newConnectionString);

    /**
     * For use in mongos which needs notifications about changes to shard replset membership to
     * update the config.shards collection.
     *
     * This is expected to be run in a brand new thread.
     */
    static void replicaSetChangeConfigServerUpdateHook(const std::string& setName,
                                                       const std::string& newConnectionString);
};

}  // namespace mongo
