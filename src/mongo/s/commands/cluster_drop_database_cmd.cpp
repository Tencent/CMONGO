/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"


#include "mongo/base/status.h"
#include "mongo/db/commands.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/client/shard_registry.h"

namespace mongo {

using std::shared_ptr;

namespace {

class DropDatabaseCmd : public Command {
public:
    DropDatabaseCmd() : Command("dropDatabase") {}

    virtual bool slaveOk() const {
        return true;
    }

    virtual bool adminOnly() const {
        return false;
    }

    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }

    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::dropDatabase);
        out->push_back(Privilege(ResourcePattern::forDatabaseName(dbname), actions));
    }

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        // Disallow dropping the config database from mongos
        if (dbname == "config" || dbname == "admin" || dbname == "local") {
            return appendCommandStatus(
                result, Status(ErrorCodes::IllegalOperation, "Cannot drop the config or admin or local database"));
        }

        BSONElement e = cmdObj.firstElement();

        if (!e.isNumber() || e.number() != 1) {
            errmsg = "invalid params";
            return 0;
        }

        // Refresh the database metadata
        grid.catalogCache()->invalidate(dbname);

        StatusWith<shared_ptr<DBConfig>> status = grid.catalogCache()->getDatabase(txn, dbname);
        if (!status.isOK()) {
            if (status == ErrorCodes::NamespaceNotFound) {
                result.append("info", "database does not exist");
                return true;
            }

            return appendCommandStatus(result, status.getStatus());
        }

		const auto& db = status.getValue();
		if (!db->isShardingEnabled()) {
			const auto shard = grid.shardRegistry()->getShard(txn, db->getPrimaryId());
			ShardConnection conn(shard->getConnString(), "");
			BSONObj res;
			bool ok = conn->runCommand(dbname, cmdObj, res, options);
			conn.done();
			result.appendElements(res);
			return ok;
		}

        log() << "DROP DATABASE: " << dbname;
		Status s = grid.catalogManager(txn)->dropDatabase(txn, dbname);
		if (!s.isOK()) {
			return appendCommandStatus(result, s);
		}

        result.append("dropped", dbname);
        return true;
    }

} clusterDropDatabaseCmd;

}  // namespace
}  // namespace mongo
