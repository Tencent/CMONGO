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
#include <vector>
#include <string>
#include "mongo/unittest/unittest.h"
#include "mongo/util/cmongo_shard_helpers.h"
#include "mongo/util/cgo/export.h"
#include "mongo/util/stringutils.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"

using namespace cmongo;
using namespace mongo;
using std::string;
using std::vector;

namespace {
vector<string> valuesOfBSONFromGolang(const BSONObj& obj) {
	GoString s = {obj.objdata(), obj.objsize()};
	char * buf = new char[1024];
	memset(buf, 0, 1024);
	GoSlice r = {static_cast<void *>(buf), 1024, 1024};
	TestFormatBsonValues(s, &r);
	vector<string> ret;
	splitStringDelim(string(buf), &ret, ',');
	delete []buf;
	return ret;
}
}

TEST(CMONGO_SHARD_HELPERS, formatBool) {
	ASSERT_EQUALS("true", formatBool(true));
	ASSERT_EQUALS("false", formatBool(false));

	BSONObj obj = BSON("a"<<true<<"b"<<false);
	auto values = valuesOfBSONFromGolang(obj);
	ASSERT_EQUALS(2, values.size());
	ASSERT_EQUALS(values[0], "true");
	ASSERT_EQUALS(values[1], "false");
}

TEST(CMONGO_SHARD_HELPERS, formatFloat64) {
	const double PI = 3.14159265358979323846264L;
	BSONObj obj = BSON("a"<< PI);
	auto values = valuesOfBSONFromGolang(obj);
	ASSERT_EQUALS(1, values.size());
	ASSERT_EQUALS(values[0], formatFloat64(PI));
	ASSERT_NOT_EQUALS(values[0], formatFloat32(PI));
}

TEST(CMONGO_SHARD_HELPERS, formatOID) {
	OID o = OID::gen();
	BSONObj obj = BSON("a"<< o);
	auto values = valuesOfBSONFromGolang(obj);
	ASSERT_EQUALS(1, values.size());
	ASSERT_EQUALS(values[0], formatOID(o));
}

TEST(CMONGO_SHARD_HELPERS, formatBinary) {
    const unsigned char value2[] = {0x00,
                                    0x9D,
                                    0x15,
                                    0xA3,
                                    0x3B,
                                    0xCC,
                                    0x46,
                                    0x60,
                                    0x90,
                                    0x45,
                                    0xEF,
                                    0x54,
                                    0x77,
                                    0x8A,
                                    0x87,
                                    0x0C};
	for (size_t i = 0; i <= 5; i++) {
		BSONObjBuilder builder;
		builder.appendBinData("a", sizeof(value2), static_cast<BinDataType>(i), &value2[0]);
		BSONObj obj = builder.done();
		if (i == BinDataGeneral || i == ByteArrayDeprecated) {
			ASSERT_TRUE(validateShardKeys(obj).isOK());
		} else {
			ASSERT_FALSE(validateShardKeys(obj).isOK());
			continue;
		}
		auto values = valuesOfBSONFromGolang(obj);
		ASSERT_EQUALS(1, values.size());
		ASSERT_EQUALS(values[0], cmongoString(obj.getField("a")));
	}
}

TEST(CMONGO_SHARD_HELPERS, cmongoString) {
	BSONObj obj = BSON("a"<< "12345");
	ASSERT_TRUE(cmongoString(obj.getField("a")) == "12345");
	obj = BSON("a"<< 12345);
	ASSERT_TRUE(cmongoString(obj.getField("a")) == "12345");
	obj = BSON("a"<< 123456789101112LL);
	ASSERT_TRUE(cmongoString(obj.getField("a")) == "123456789101112");
}
