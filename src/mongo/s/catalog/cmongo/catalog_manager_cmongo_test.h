//catalog_manager_cmongo_test.h
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

#ifndef __MONGO_S_CATALOG_CMONGO_CATALOG_MANAGER_CMONGO_TEST_H__
#define __MONGO_S_CATALOG_CMONGO_CATALOG_MANAGER_CMONGO_TEST_H__

#include "mongo/s/catalog/cmongo/catalog_manager_cmongo.h"
#include "mongo/grpc/masterproto/master.pb.h"
#include "mongo/grpc/masterproto/master.grpc.pb.h"
#include "mongo/grpc/cmongoproto/cmongo.pb.h"
#include "mongo/base/status.h"

namespace mongo {
class CatalogManagerCmongoTest {
public:
	CatalogManagerCmongoTest() = default;
	virtual ~CatalogManagerCmongoTest() = default;
	Status injectRoute(const masterproto::GetClusterRoutesRsp& pbobj,
		CatalogManagerCmongo* value);
	Status injectShard(const masterproto::GetClusterInfoRsp& pbobj,
		 CatalogManagerCmongo* value);
};
}
#endif //__MONGO_S_CATALOG_CMONGO_CATALOG_MANAGER_CMONGO_TEST_H__
