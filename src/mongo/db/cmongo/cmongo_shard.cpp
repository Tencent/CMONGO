//cmongo_shard.cpp
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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl  

#include <iostream>
#include "mongo/util/log.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/cmongo/cmongo_shard.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/bson/mutable/element.h"
#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/base/status.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/db/matcher/matchable.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;

namespace mongo {

CMongoShardingState* _cmongo_shard_state = NULL;

MONGO_INITIALIZER(CMongoShardingStateInit)(InitializerContext* context) {
    _cmongo_shard_state = new CMongoShardingState();
	return Status::OK();
}

CMongoShardingState* getCMongoShardingState() {
    return _cmongo_shard_state;
}

CMongoShardingState::CMongoShardingState()
		:_operationLock("CMongoShardingStateLock") {
	_namespace = "admin.cmongo_shard";
}

shared_ptr<CollectionMetadata> CMongoShardingState::getCollectionMetadata(const string& ns) {
	if (ns == _namespace) {
		// NOTE(deyukong): when removing data from itself use DBDirectClient,
		// OpObserver will getCollectionMetadata, CMongoShardingState::remove has 
		// already taken wlock, if we donot check it first,
        // deadlock between CMongoShardingState::remove and CMongoShardingState::getCollectionMetadata
		// will happen
		return nullptr;
	}
	rwlock_shared oplk(_operationLock);
    CollectionMetadataMap::const_iterator it = _collMetadata.find(ns);
    if (it == _collMetadata.end()) {
		return nullptr;
    } else {
        return it->second;
    }
}

Status CMongoShardingState::initialize(OperationContext* txn) {
    stdx::lock_guard<RWLock> lk(_operationLock);
	DBDirectClient client(txn);
	BSONObjBuilder bb;
	unique_ptr<DBClientCursor> cursor = client.query(_namespace, bb.done());
	while(cursor->more()) {
		const BSONObj& obj = cursor->next();
		const BSONElement& ele = obj["table_name"];
		if (ele.eoo()) {
			LOG(0)<<"ERROR: CMongoShardingState failed,"<<obj<<" has no table_name field";
			return Status(ErrorCodes::InternalError, str::stream()
				<<"ERROR: CMongoShardingState failed,"<<obj<<" has no table_name field");
		}
		const string ns = ele.String();
		const BSONElement& ele1 = obj["pattern"];
		if (ele1.eoo()) {
			LOG(0)<<"ERROR: CMongoShardingState failed,"<<obj<<" has no pattern field";
			return Status(ErrorCodes::InternalError, str::stream()
				<<"ERROR: CMongoShardingState failed,"<<obj<<" has no pattern field");
		}
		shared_ptr<CollectionMetadata> meta(new CollectionMetadata);
		meta->fromBSON(ele1.Obj());
		_collMetadata[ns] = meta;
	}
	return Status::OK();
}

bool CMongoShardingState::insert(OperationContext* txn, const string& ns,
		const BSONObj& pattern) {
    stdx::lock_guard<RWLock> lk(_operationLock);
	if (_collMetadata.find(ns) != _collMetadata.end()) {
		LOG(0)<<"[CMongoShardingState] ns:"<<ns<<" keypattern:"<<pattern<<" has already exists, cover old";
		return false;
	}

	DBDirectClient client(txn);

	BSONObjBuilder bb;
	bb.append("table_name", ns);
	bb.append("pattern", pattern);
	client.insert(_namespace, bb.done());

	shared_ptr<CollectionMetadata> meta(new CollectionMetadata);
	meta->fromBSON(pattern);
	_collMetadata[ns] = meta;
	return true;
}

bool CMongoShardingState::remove(OperationContext* txn, const string& ns) {
    stdx::lock_guard<RWLock> lk(_operationLock);
	if (_collMetadata.find(ns) == _collMetadata.end()) {
		return false;
	}
	DBDirectClient client(txn);
	BSONObjBuilder bb;
	bb.append("table_name", ns);
	client.remove(_namespace, bb.done());
	_collMetadata.erase(ns);
	return true;
}

BSONObj CMongoShardingState::getImmutableValues(const string& ns, const BSONObj& doc) {
	if (ns == _namespace) {
		// NOTE(deyukong): when removing data from itself use DBDirectClient,
		// OpObserver will getCollectionMetadata, CMongoShardingState::remove has 
		// already taken wlock, if we donot check it first,
        // deadlock between CMongoShardingState::remove and CMongoShardingState::getCollectionMetadata
		// will happen
		return BSONObj();
	}
	rwlock_shared oplk(_operationLock);
	BSONObjBuilder bb;
	shared_ptr<CollectionMetadata> meta;
	if (_collMetadata.find(ns) == _collMetadata.end()) {
		return BSONObj();
	} else {
		meta = _collMetadata[ns];
	}

	BSONObj shardkey = meta->getKeyPattern();
	BSONObjIterator patternIt(shardkey);
	BSONMatchableDocument matchable(doc);
	while (patternIt.more()) {
		BSONElement patternEl = patternIt.next();
		ElementPath path;
		path.init(patternEl.fieldNameStringData());
		path.setTraverseNonleafArrays(false);
		path.setTraverseLeafArray(false);
		MatchableDocument::IteratorHolder matchIt(&matchable, &path);
		if (!matchIt->more()) {
			return BSONObj();
		} else {
			BSONElement match_ele = matchIt->next().element();
			bb.appendAs(match_ele, patternEl.fieldName());
		}
	}
	return bb.obj();
}
}
