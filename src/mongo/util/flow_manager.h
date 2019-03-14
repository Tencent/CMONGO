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

#include <map>
#include <string>
#include <atomic>
#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/namespace_string.h"
#include "mongo/util/etcd_wrapper.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/util/background.h"
#include "mongo/grpc/grpc_client.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/net/sock.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {

namespace proxy_stats {
// TODO(deyukong): we can follow the trace of globalOpCounters
// globalOpCounters are used in mongos and mongod both,
// but currently we only stats in mongos side.
// so we can not integrate RequestStats into globalOpCounters
class RequestStats {
public:
	explicit RequestStats(const NamespaceString& nss)
		:_nss(nss),
		 _now(std::chrono::system_clock::now()) {
	}
	virtual ~RequestStats() = default;
protected:
	const NamespaceString _nss;
	std::chrono::time_point<std::chrono::system_clock> _now;
};

class ReadStats: public RequestStats {
public:
	explicit ReadStats(const NamespaceString& nss)
		:RequestStats(nss) {
	}
	~ReadStats() final;
};

class InsertStats: public RequestStats {
public:
	explicit InsertStats(const NamespaceString& nss)
		:RequestStats(nss) {
	}
	~InsertStats() final;
};

class UpdateStats: public RequestStats {
public:
	explicit UpdateStats(const NamespaceString& nss)
		:RequestStats(nss) {
	}
	~UpdateStats() final;
};

class DeleteStats: public RequestStats {
public:
	explicit DeleteStats(const NamespaceString& nss)
		:RequestStats(nss) {
	}
	~DeleteStats() final; 
};

class CommandStats: public RequestStats {
public:
	explicit CommandStats(const NamespaceString& nss)
		:RequestStats(nss) {
	}
	~CommandStats() final; 
};
}

struct CMongoProxyStat {
	int64_t Reads;
	int64_t Inserts;
	int64_t Updates;
	int64_t Deletes;
	int64_t Counts;
	int64_t Aggregates;
	int64_t Commands;
	int64_t Successes;
	int64_t FullPool;
	int64_t ClientErrs;
	int64_t ServerErrs;
	int64_t Timeouts;
	int64_t Ten;
	int64_t Fifty;
	int64_t Hundred;
	int64_t TotalConnect;
	int64_t TotalMaxConnect;
	BSONObj dump() const;
};

struct CMongoConnectionStat {
    HostAndPort client;
    UserName user;
};

enum DumpConnType { dumpConnNone, dumpConnAggregated, dumpConnAll };

struct Ratio {
	int32_t Read;
    int32_t Insert;
	int32_t Update;
	int32_t Delete;
	int32_t Count;
	int32_t Aggregate;
	std::string Stat;
	bool operator ==(const Ratio& v) const;
};

class FlowManager {
	MONGO_DISALLOW_COPYING(FlowManager);
public:
	FlowManager(const std::string& meta_url, const std::string& cluster);
	virtual ~FlowManager() = default;
	virtual bool hitFlowControlForRead(OperationContext* txn, const NamespaceString& ns);
	virtual bool hitFlowControlForWrite(OperationContext* txn, const NamespaceString& ns);
	virtual std::string getGlobalStat(OperationContext *txn);
	Status startup(OperationContext* txn);
	bool updateFilter(const std::map<std::string, Ratio>& newone);
	bool updateMonitorAddr(const std::string& addr);
	bool updateMasterAddr(const std::string& addr);
	void onReads(const NamespaceString& ns, Milliseconds milsecs);
	void onInserts(const NamespaceString& ns, Milliseconds milsecs);
	void onUpdates(const NamespaceString& ns, Milliseconds milsecs);
	void onDeletes(const NamespaceString& ns, Milliseconds milsecs);
	void onCounts(const NamespaceString& ns, Milliseconds milsecs);
	void onAggregrates(const NamespaceString& ns, Milliseconds milsecs);
	void onCommands(const NamespaceString& ns, Milliseconds milsecs);
	void onSuccesses(const NamespaceString& ns);
	void onServerError( const NamespaceString& ns);
	void onClientError( const NamespaceString& ns);
	bool needTicket(const std::string& addr) const;
	BSONObj dump() const;
    void dump(BSONObjBuilder* bbp, DumpConnType dumpConnType = dumpConnNone, bool internal = false) const;
    void dump_inlock(BSONObjBuilder* bbp, DumpConnType dumpConnType = dumpConnNone, bool internal = false) const;
    void dumpConnection_inlock(BSONObjBuilder* bbp, DumpConnType dumpConnType, bool internal) const;
	// this function may throw exceptions
	virtual BSONObj getFilter();
	virtual std::string getMonitorAddr();
	virtual std::string getMasterAddr();
	bool inShutdown() const;
	void shutdown();

    void onConnected(long long connectionID,
                     const HostAndPort & client);
    void onDisconnected(long long connectionID);
    void onAuthenticate(long long connectionID,
                        const UserName & user);

private:
	void _statsTime_inlock(const NamespaceString&, Milliseconds);
	std::map<std::string, Ratio> _opRatioMap;
	std::unique_ptr<etcd::EtcdWrapper> _handler;
	std::unique_ptr<BackgroundJob> _bg;
	const std::string _metaUrl;
	const std::string _cluster;
	std::string _monitor;
	std::string _master;
    mutable stdx::mutex _mutex;
	bool _started;
	bool _inShutdown;
	CMongoProxyStat _statsAll;
	std::map<std::string, CMongoProxyStat> _tableStats;
	std::map<long long, CMongoConnectionStat> _connectionStats;
};
}
