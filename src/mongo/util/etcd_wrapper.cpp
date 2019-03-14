/*

 The MIT License (MIT)

 Copyright (C) 2017 DeyuKong

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.

*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include <vector>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include "etcd.h"
#include "etcd_wrapper.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/log.h"
#include "mongo/bson/json.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/util/stringutils.h"

using std::string;
using std::vector;
using std::stringstream;

using namespace mongoutils;

namespace etcd {
namespace {
std::vector<std::string> formatMetaUrl(const std::string& metaUrl) {
	std::vector<std::string> vec;
	mongo::splitStringDelim(metaUrl, &vec, ',');
	std::vector<std::string> ret;
	std::string header = "http://";
	for (auto& s: vec) {
		if (s.find(header) == 0) {
			ret.push_back(s.substr(header.length()));
		} else {
			ret.push_back(s);
		}
	}
	return ret;
}
}

struct InvalidHostError: public std::runtime_error {
	InvalidHostError(const string& error)
		:std::runtime_error("etcd exception"),
		 error(error)
		{}
	virtual const char* what() const throw() {
		return error.c_str();
	}
	std::string error;
};

EtcdWrapper::EtcdWrapper(const std::string& metaUrl)
	:EtcdWrapper(formatMetaUrl(metaUrl)) {
}

EtcdWrapper::EtcdWrapper(const vector<string>& hosts) {
	for (auto& host :hosts) {
		string ip, port;
		if (!str::splitOn(host, ':', ip, port)) {
			stringstream sstream;
			string result;
			sstream << "invalid etcdhost:";
			sstream << host;
			sstream >> result;
			throw InvalidHostError(result);
		}
		uint32_t port_num = str::toUnsigned(port);
		if (port_num == 0) {
			stringstream sstream;
			string result;
			sstream << "invalid etcdhost:";
			sstream << host;
			sstream >> result;
			throw InvalidHostError(result);
		}
		// TODO(deyukong): configure curl timeout
		clients_.push_back(Client<SimpleReply>(ip, port_num));
	}
}

string EtcdWrapper::GetRaw(const string& key) {
	for (auto& c :clients_) {
		string ret;
		try {
			ret = c.Get(key).ToString();
		} catch (const std::exception& e) {
			LOG(0)<<"Get "<<key<<"failed, from etcd:"<<c.GetUrl();
			continue;
		}
		return ret;
	}
	throw ClientException("query all etcd hosts failed");
}

string EtcdWrapper::GetValue(const string& key) {
	string v = GetRaw(key);
	mongo::BSONObj obj = mongo::fromjson(v);
	mongo::BSONElement e = obj["node"]["value"];
	if (e.eoo()) {
		throw ClientException(str::stream() << "invalid format of etcdnode:" << v);
	}
	return e.str();
}
}
