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

#ifndef __MONGO_UTIL_ETCD_WRAPPER_H_INCLUDED__
#define __MONGO_UTIL_ETCD_WRAPPER_H_INCLUDED__

#include <vector>
#include "etcd.h"

namespace etcd {

class SimpleReply :public std::string {
	public:
		SimpleReply(const std::string& in) {
			data_ = in;
		}
		SimpleReply(const std::string& in1, const std::string& in2) {
			data_ = in1 + in2;
		}
		std::string ToString() const {
			return data_;
		}

	private:
		std::string data_;
};

class EtcdWrapper {
	public:
		EtcdWrapper(const std::string& metaUrl);
	    EtcdWrapper(const std::vector<std::string>& svrs);
		std::string GetRaw(const std::string&);
		std::string GetValue(const std::string&);
	private:
		std::vector<Client<SimpleReply>> clients_;
};

}

#endif // __ETCD_WRAPPER_H_INCLUDED__
