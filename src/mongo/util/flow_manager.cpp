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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include <algorithm>
#include "mongo/util/flow_manager.h"
#include "mongo/util/log.h"
#include "mongo/bson/json.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/db/client.h"
#include "mongo/util/net/listen.h"
#include "mongo/s/grid.h"

namespace mongo {
namespace {

#define LLL(a) ((static_cast<long long int>(a)))

// TODO(deyukong): how many times a constructor is called
inline std::string lower(const std::string& a) {
	std::string ret = a;
	std::transform(a.begin(), a.end(), ret.begin(),
		[](unsigned char c) { return std::tolower(c);});
	return ret;
}

const std::string globalNs = "$global.$global";
typedef std::map<std::string, Ratio> FlowMap;
typedef std::map<std::string, CMongoProxyStat> TableStatsMap;
class FlowEtcdWatcher: public BackgroundJob {
public:
	FlowEtcdWatcher(FlowManager* manager)
		:_manager(manager),
		 _name("FlowEtcdWatcher") {
	}

	virtual std::string name() const {
		return _name;
	}

	virtual void run() {
		Client::initThread(_name.c_str());
		while (!_manager->inShutdown()) {
			updateManager();
			sleepmillis(1000*60);
		}
	}

	void updateManager() {
		try {
			std::string monitorAddr = _manager->getMonitorAddr();
			_manager->updateMonitorAddr(monitorAddr);
			std::string masterAddr = _manager->getMasterAddr();
			_manager->updateMasterAddr(masterAddr);
		} catch(const DBException& e) {
			LOG(0) << "WARN:update MonitorAddr faied:" << e.what();
		} catch (const std::runtime_error& e) {
			LOG(0) << "WARN:update MonitorAddr faied:" << e.what();
		}

		try {
			BSONObj obj = _manager->getFilter();
			if (obj.isEmpty()) {
				_manager->updateFilter(FlowMap());
				return;
			}
			BSONObjIterator it(obj);
			while(it.more()) {
				BSONElement element = it.next();
				if (element["Ns"].eoo()) {
					LOG(0) << "ERROR:parse etcd filter info with no ns" << element;
				} else if (element["Ns"].str() != globalNs) {
					LOG(1) << "ignore non-global filter info:" << element;
				} else {
					std::string stat_string = "flow control."; // default
					if (!element["Stat"].eoo()) {
						stat_string = element["Stat"].String();
					}
					Ratio ratio = {element["Read"].numberInt(),
							 element["Insert"].numberInt(),
							 element["Update"].numberInt(),
							 element["Delete"].numberInt(),
                             element["Count"].numberInt(),
                             element["Aggregate"].numberInt(),
							 stat_string};
					auto tempmap = FlowMap();
					tempmap.insert(std::pair<std::string, Ratio>(globalNs, ratio));
					_manager->updateFilter(tempmap);
					break;
				}
			}
			LOG(0) << "WARN:no global flowcontrol info found";
		} catch (const DBException& e) {
			LOG(0) << "WARN:update FlOWManager faied:" << e.what();
		} catch (const std::runtime_error& e) {
			LOG(0) << "WARN:update FlOWManager faied:" << e.what();
		}
	}
private:
	FlowManager* _manager;
	std::string _name;
};
}

BSONObj CMongoProxyStat::dump() const {
	return BSON(lower("Reads") << LLL(Reads)
				<< lower("Inserts") << LLL(Inserts)
				<< lower("Updates") << LLL(Updates)
				<< lower("Deletes") << LLL(Deletes)
				<< lower("Counts") << LLL(Counts)
				<< lower("Aggregates") << LLL(Aggregates)
				<< lower("Commands") << LLL(Commands)
				<< lower("Successes") << LLL(Successes)
				<< lower("FullPool") << LLL(FullPool)
				<< lower("ClientErrs") << LLL(ClientErrs)
				<< lower("ServerErrs") << LLL(ServerErrs)
				<< lower("Timeouts") << LLL(Timeouts)
				<< lower("Ten") << LLL(Ten)
				<< lower("Fifty") << LLL(Fifty)
				<< lower("Hundred") << LLL(Hundred)
				<< lower("TotalConnect") << LLL(Listener::globalTicketHolder.used())
				<< lower("TotalMaxConnect") << LLL(Listener::globalTicketHolder.outof()));
}

bool Ratio::operator==(const Ratio& v) const {
	return Read == v.Read &&
			Insert == v.Insert &&
			Update == v.Update &&
			Delete == v.Delete &&
			Count == v.Count &&
			Aggregate == v.Aggregate &&
			Stat == v.Stat;
}

BSONObj FlowManager::getFilter() {
	// GetValue may throw exception
	const std::string data = _handler->GetValue(str::stream() << "/filter/" << _cluster);
	if (data == "null") {
		return BSONObj();
	}
	// fromjson may throw exception
	return fromjson(data);
}

std::string FlowManager::getMonitorAddr() {
	return _handler->GetValue(str::stream() << "/monitor_addr");
}

std::string FlowManager::getMasterAddr() {
	std::string masterIpPort =  _handler->GetValue(str::stream() << "/master/primary");
	std::size_t commaPos = masterIpPort.find(":");
	if(commaPos == std::string::npos) {
		// no port info 
		return masterIpPort;
	}
	return masterIpPort.substr(0, commaPos);
}

FlowManager::FlowManager(const std::string& meta_url, const std::string& cluster)
	:_metaUrl(meta_url),
	 _cluster(cluster),
	 _started(false),
	 _inShutdown(false) {
	memset(&_statsAll, 0, sizeof (CMongoProxyStat));
}

bool FlowManager::updateFilter(const FlowMap& newone) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	if (newone == _opRatioMap) {
		return false;
	}
	LOG(0) << "flow control filter info got updated";
	_opRatioMap = newone;
	return true;
}

bool FlowManager::updateMonitorAddr(const std::string& newone) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	if (newone == _monitor) {
		return false;
	}
	LOG(0) << "flow control monitor addr got updated:" << newone;
	_monitor = newone;
	return true;
}

bool FlowManager::updateMasterAddr(const std::string& newone) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	if (newone == _master) {
		return false;
	}
	LOG(0) << "flow control master addr got updated:" << newone;
	_master = newone;
	return true;
}

void FlowManager::shutdown() {
	_inShutdown = true;
}

bool FlowManager::inShutdown() const {
	return _inShutdown;
}

void FlowManager::onConnected(long long connectionID,
                              const HostAndPort & client) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    std::map<long long, CMongoConnectionStat>::iterator iter = _connectionStats.find(connectionID);
    if (iter == _connectionStats.end()) {
        CMongoConnectionStat stat;
        stat.client = client;
        _connectionStats[connectionID] = stat;
    } else {
        iter->second.client = client;
    }
}

void FlowManager::onDisconnected(long long connectionID) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _connectionStats.erase(connectionID);
}

void FlowManager::onAuthenticate(long long connectionID,
                                 const UserName & user) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    std::map<long long, CMongoConnectionStat>::iterator iter = _connectionStats.find(connectionID);
    if (iter == _connectionStats.end()) {
        LOG(0) << "flow control connection " << connectionID << " is not found";
    } else {
        iter->second.user = user;
    }
}

bool FlowManager::hitFlowControlForRead(OperationContext *txn, const NamespaceString& ns) {
	// NOTE(deyukong): no need to ban read currently
	return false;
}

bool FlowManager::hitFlowControlForWrite(OperationContext *txn, const NamespaceString& ns) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	auto it = _opRatioMap.find(globalNs);
	if (it == _opRatioMap.end()) {
		return false;
	}
	// NOTE(deyukong): in go-imple proxy2, a random strategy is used
	// here we give it up and do strict ban
	return it->second.Insert == 100;
}

std::string FlowManager::getGlobalStat(OperationContext *txn) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	auto it = _opRatioMap.find(globalNs);
	if (it == _opRatioMap.end()) {
		return "";
	}
	return it->second.Stat;
}

void FlowManager::_statsTime_inlock(const NamespaceString& ns, Milliseconds milsecs) {
	if (milsecs.count() > 100) {
		_statsAll.Hundred += 1;
		_tableStats[ns.ns()].Hundred += 1;
	} else if (milsecs.count() > 50) {
		_statsAll.Fifty += 1;
		_tableStats[ns.ns()].Fifty += 1;
	} else if (milsecs.count() > 10) {
		_statsAll.Ten += 1;
		_tableStats[ns.ns()].Ten += 1;
	}
}

void FlowManager::onReads(const NamespaceString& ns, Milliseconds milsecs) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.Reads += 1;
	_tableStats[ns.ns()].Reads += 1;
	_statsTime_inlock(ns, milsecs);
}

void FlowManager::onInserts(const NamespaceString& ns, Milliseconds milsecs) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.Inserts += 1;
	_tableStats[ns.ns()].Inserts += 1;
	_statsTime_inlock(ns, milsecs);
}

void FlowManager::onUpdates(const NamespaceString& ns, Milliseconds milsecs) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.Updates += 1;
	_tableStats[ns.ns()].Updates += 1;
	_statsTime_inlock(ns, milsecs);
}

void FlowManager::onDeletes(const NamespaceString& ns, Milliseconds milsecs) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.Deletes += 1;
	_tableStats[ns.ns()].Deletes += 1;
	_statsTime_inlock(ns, milsecs);
}

void FlowManager::onCounts(const NamespaceString& ns, Milliseconds milsecs) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.Counts += 1;
	_tableStats[ns.ns()].Counts += 1;
	_statsTime_inlock(ns, milsecs);
}

void FlowManager::onAggregrates(const NamespaceString& ns, Milliseconds milsecs) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.Aggregates += 1;
	_tableStats[ns.ns()].Aggregates += 1;
	_statsTime_inlock(ns, milsecs);
}

void FlowManager::onCommands(const NamespaceString& ns, Milliseconds milsecs) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.Commands += 1;
	_tableStats[ns.ns()].Commands += 1;
	_statsTime_inlock(ns, milsecs);
}

void FlowManager::onSuccesses(const NamespaceString& ns) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.Successes += 1;
	_tableStats[ns.ns()].Successes += 1;
}

void FlowManager::onServerError(const NamespaceString& ns) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.ServerErrs += 1;
	_tableStats[ns.ns()].ServerErrs += 1;
}

void FlowManager::onClientError(const NamespaceString& ns) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	_statsAll.ClientErrs += 1;
	_tableStats[ns.ns()].ClientErrs += 1;
}

void FlowManager::dump_inlock(BSONObjBuilder* bbp, DumpConnType dumpConnType, bool internal) const {
	uint64_t startTime = curTimeMicros64();
	bbp->append(lower("Ok"), 1);
	bbp->appendElements(_statsAll.dump());
	BSONObjBuilder bb1;
	const uint32_t table_limits = 1024;
	uint32_t cnt = 0;
	for (auto& stat : _tableStats) {
		bb1.append(stat.first, stat.second.dump());
		if (++cnt >= table_limits) {
			LOG(0) << "WARN:FlowManager::dump_inlock too many tables:" << _tableStats.size();
			break;
		}
	}
	bbp->append(lower("TableStats"), bb1.obj());

	dumpConnection_inlock(bbp, dumpConnType, internal);

	LOG(0) << "FlowManager::dump_inlock cost:" << curTimeMicros64() - startTime << " microseconds";
}

void FlowManager::dumpConnection_inlock(BSONObjBuilder * bbp, DumpConnType dumpConnType, bool internal) const {
    switch (dumpConnType) {
    case dumpConnAll:
    {
        BSONArrayBuilder bbConn(bbp->subarrayStart(lower("ConnStats")));

        for (auto & stat : _connectionStats) {
            const HostAndPort & client = stat.second.client;
            if (!needTicket(client.host()) && !internal) {
                continue;
            }
            BSONObjBuilder builder(bbConn.subobjStart());
            builder.append(lower("connection"), LLL(stat.first));
            builder.append(lower("Client"), client.toString());
            builder.append(lower("User"), stat.second.user.toString());
            builder.doneFast();
        }

        bbConn.doneFast();
        break;
    }
    case dumpConnAggregated:
    {
        std::map<std::string, int64_t> connectionCounts;
        std::map<std::string, int64_t> databaseCounts;
        std::map<std::string, int64_t> userCounts;
        for (auto & stat : _connectionStats) {
            auto host = stat.second.client.host();
            if (!needTicket(host) && !internal) {
                continue;
            }
            auto connIter = connectionCounts.find(host);
            if (connIter == connectionCounts.end()) {
                connectionCounts[host] = 1;
            } else {
                ++(connIter->second);
            }
            if (!stat.second.user.getFullName().empty()) {
                auto database = stat.second.user.getDB().toString();
                auto dbIter = databaseCounts.find(database);
                if (dbIter == databaseCounts.end()) {
                    databaseCounts[database] = 1;
                } else {
                    ++(dbIter->second);
                }
                auto user = stat.second.user.getUser().toString();
                auto userIter = userCounts.find(user);
                if (userIter == userCounts.end()) {
                    userCounts[user] = 1;
                } else {
                    ++(userIter->second);
                }
            }
        }

        BSONObjBuilder bbConn(bbp->subobjStart(lower("ConnStats")));

        // Client block
        BSONArrayBuilder bbClient(bbConn.subarrayStart(lower("Clients")));
        for (auto connectionCount : connectionCounts) {
            BSONObjBuilder connBuilder(bbClient.subobjStart());
            connBuilder.append(lower("Client"), connectionCount.first);
            connBuilder.append(lower("Count"), LLL(connectionCount.second));
            connBuilder.doneFast();
        }
        bbClient.doneFast();

        // Database block
        BSONArrayBuilder bbDatabase(bbp->subarrayStart(lower("DatabaseStats")));
        for (auto databaseCount : databaseCounts) {
            BSONObjBuilder dbBuilder(bbDatabase.subobjStart());
            dbBuilder.append(lower("Database"), databaseCount.first);
            dbBuilder.append(lower("Count"), LLL(databaseCount.second));
            dbBuilder.doneFast();
        }
        bbDatabase.doneFast();

        // User block
        BSONArrayBuilder bbUser(bbp->subarrayStart(lower("UserStats")));
        for (auto userCount : userCounts) {
            BSONObjBuilder userBuilder(bbUser.subobjStart());
            userBuilder.append(lower("User"), userCount.first);
            userBuilder.append(lower("Count"), LLL(userCount.second));
            userBuilder.doneFast();
        }
        bbUser.doneFast();

        bbConn.doneFast();
        break;
    }
    default:
        break;
    }
}

void FlowManager::dump(BSONObjBuilder* bbp, DumpConnType dumpConnType, bool internal) const {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	dump_inlock(bbp, dumpConnType, internal);
}

bool FlowManager::needTicket(const std::string& addr) const {
	if (addr.empty() || _monitor.empty()) {
		return true;
	}

	if (addr == _master) {
		LOG(1) << "filter the master addr: " << addr;
		return false;
	}

	if (_monitor.find(addr, 0) != std::string::npos) {
		LOG(1) << "filter the monitor addr: " << addr;
		return false;
	}

	return true;
}

BSONObj FlowManager::dump() const {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	BSONObjBuilder bb;
	dump_inlock(&bb);
	return bb.obj();
}

Status FlowManager::startup(OperationContext *txn) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	if (_started) {
		return Status::OK();
	}
	try {
		_handler = std::unique_ptr<etcd::EtcdWrapper>(new etcd::EtcdWrapper(_metaUrl));
	} catch (std::runtime_error& e) {
		return Status(ErrorCodes::InternalError, e.what());
	}
	if (_bg.get() == nullptr) {
		_bg = std::unique_ptr<FlowEtcdWatcher>(new FlowEtcdWatcher(this));
		_bg->go();
	}
	_started = true;
	return Status::OK();
}

namespace proxy_stats {
ReadStats::~ReadStats() {
	Milliseconds milliseconds = std::chrono::duration_cast<Milliseconds>(std::chrono::system_clock::now() - _now);
	grid.getFlowManager()->onReads(_nss, milliseconds);
}

InsertStats::~InsertStats() {
	Milliseconds milliseconds = std::chrono::duration_cast<Milliseconds>(std::chrono::system_clock::now() - _now);
	grid.getFlowManager()->onInserts(_nss, milliseconds);
}

UpdateStats::~UpdateStats() {
	Milliseconds milliseconds = std::chrono::duration_cast<Milliseconds>(std::chrono::system_clock::now() - _now);
	grid.getFlowManager()->onUpdates(_nss, milliseconds);
}

DeleteStats::~DeleteStats() {
	Milliseconds milliseconds = std::chrono::duration_cast<Milliseconds>(std::chrono::system_clock::now() - _now);
	grid.getFlowManager()->onDeletes(_nss, milliseconds);
}

CommandStats::~CommandStats() {
	Milliseconds milliseconds = std::chrono::duration_cast<Milliseconds>(std::chrono::system_clock::now() - _now);
	grid.getFlowManager()->onCommands(_nss, milliseconds);
}
} // namespace proxy_stats
} // namespace mongo
