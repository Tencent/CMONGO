// flow_manager_test.cpp

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
#include <iostream>

#include "mongo/util/flow_manager.h"
#include "mongo/unittest/unittest.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/json.h"
#include "mongo/db/operation_context_noop.h"

namespace mongo {

using namespace std;

typedef std::chrono::duration<int,std::milli> milliseconds_type;

class FlowManagerMock: public FlowManager {
public:
	FlowManagerMock()
		:FlowManager("", "") {
	}
	BSONObj getFilter() final {
		return fromjson("[{'Ns':'$global.$global','Read':0,'Insert':100,'Update':100,'Delete':0,'Count':0,'Aggregate':0}]");
	}
	std::string getMonitorAddr() final {
		return "127.0.0.1";
	}
};

class FlowManagerMock1: public FlowManager {
public:
	FlowManagerMock1()
		:FlowManager("", "") {
	}
	BSONObj getFilter() final {
		return BSONObj();
	}
	std::string getMonitorAddr() final {
		return "127.0.0.1";
	}
};


TEST(FLOW_MANAGER, generic) {
	FlowManagerMock mock;	
	BSONObj obj = mock.getFilter();
	ASSERT_TRUE(obj.couldBeArray());

    BSONObjIterator i(obj);
	while(i.more()) {
		BSONElement element = i.next();
		ASSERT_TRUE(element["Ns"].str() == "$global.$global");
		ASSERT_TRUE(element["Read"].numberInt() == 0);
		ASSERT_TRUE(element["Insert"].numberInt() == 100);
		ASSERT_TRUE(element["Update"].numberInt() == 100);
		ASSERT_TRUE(element["Delete"].numberInt() == 0);
		ASSERT_TRUE(element["Count"].numberInt() == 0);
		ASSERT_TRUE(element["Aggregate"].numberInt() == 0);
		Ratio v = {
			element["Read"].numberInt(),
			element["Insert"].numberInt(),
			element["Update"].numberInt(),
			element["Delete"].numberInt(),
			element["Count"].numberInt(),
			element["Aggregate"].numberInt()
		};
		auto m = std::map<std::string, Ratio>();
		m.insert(std::pair<std::string, Ratio>("$global.$global", v));
		ASSERT_TRUE(mock.updateFilter(m));
		ASSERT_FALSE(mock.updateFilter(m));

        OperationContextNoop txn;
        ASSERT_TRUE(mock.hitFlowControlForWrite(&txn, NamespaceString("$global.$global")));
	}

	FlowManagerMock1 mock1;	
	BSONObj obj1 = mock1.getFilter();
	ASSERT_TRUE(obj1.isEmpty());
	ASSERT_FALSE(mock1.updateFilter(std::map<std::string, Ratio>()));

	OperationContextNoop txn;
	ASSERT_FALSE(mock1.hitFlowControlForWrite(&txn, NamespaceString("$global.$global")));


}

TEST(FLOW_MANAGER, stats) {
	FlowManagerMock mock;	
	mock.onReads(NamespaceString("test.abc"), milliseconds_type(51));
	BSONObj obj = mock.dump();
	// std::cout<< tojson(obj) << endl;
	ASSERT_TRUE(obj["reads"].numberInt() == 1);
	ASSERT_TRUE(obj["tablestats"]["test.abc"]["reads"].numberInt() == 1);
}

TEST(FLOW_MANAGER, monitor) {
	FlowManagerMock mock;	
	ASSERT_TRUE(mock.updateMonitorAddr("127.0.0.1"));
	ASSERT_FALSE(mock.needTicket("127.0.0.1"));
	ASSERT_TRUE(mock.needTicket("127.0.0.2"));

	ASSERT_TRUE(mock.updateMonitorAddr("127.0.0.2"));
	ASSERT_FALSE(mock.updateMonitorAddr("127.0.0.2"));
	ASSERT_FALSE(mock.needTicket("127.0.0.2"));
}
}
