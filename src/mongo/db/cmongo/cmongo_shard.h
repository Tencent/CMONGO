//cmongo_shard.h
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

#include "mongo/util/concurrency/rwlock.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/db/jsobj.h" 

namespace mongo {

class CMongoShardingState {
    MONGO_DISALLOW_COPYING(CMongoShardingState);

public:
	CMongoShardingState();
	~CMongoShardingState();
	Status initialize(OperationContext* txn);
	bool insert(OperationContext* txn, const std::string& ns, const BSONObj& pattern);
	bool remove(OperationContext* txn, const std::string& ns);
	std::shared_ptr<CollectionMetadata> getCollectionMetadata(const std::string& ns);
    BSONObj getImmutableValues(const std::string& ns, const BSONObj& doc);
private:
	RWLock _operationLock;
	std::string _namespace;
    typedef std::map<std::string, std::shared_ptr<CollectionMetadata>> CollectionMetadataMap;
	CollectionMetadataMap _collMetadata;
};

CMongoShardingState* getCMongoShardingState();
}
