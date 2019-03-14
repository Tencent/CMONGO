/*

 The MIT License (MIT)

 Copyright (C) 2015 Suryanathan Padmanabhan

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

/**
 * @file etcd.hpp
 * @brief c++ language binding for etcd
 * @author Suryanathan Padmanabhan
 * @version v0.1
 * @date 2015-10-25
 *
 * C++ binding to access etcd API.
 *
 * Dependencies: libcurl, C++11 
 *
 * The following abstractions are available to access the etcd API:
 *
 * a) etcd::Client implements the main client interface.
 * b) etcd::Watch implements a callback based watchdog to watch etcd key and
 * directory changes.
 *
 * Response from etcd is JSON. This implementation is agnostic to any specific
 * json library. If you already have a json library in your project, just
 * implement a wrapper simalar to one in "rapid_reply.hpp". If you would like
 * to pick another JSON implementation, here:
 * https://github.com/miloyip/nativejson-benchmark would be a good place to start.
 *
 */

#include <curl/curl.h>
#include "etcd.h"

namespace etcd {
namespace internal {
Curl::
Curl()
  :handle_(NULL),
   enable_header_(false) {

    curl_global_init(CURL_GLOBAL_ALL);
    handle_ = curl_easy_init();
    if (! handle_) 
        throw CurlUnknownException("failed init");
}

Curl::
~Curl() {
    curl_easy_cleanup(handle_);
}

//------------------------------- OPERATIONS ---------------------------------

std::string Curl::
Get(const std::string& url) {
    _ResetHandle();
    _SetGetOptions(url);

    CURLcode err = curl_easy_perform(handle_);
    _CheckError(err, "easy perform");

    return write_stream_.str();
}

std::string Curl::
Set(const std::string& url,
    const std::string& type,
    const CurlOptions& options) {

    _ResetHandle();
    _SetPostOptions(url, type, options);

    CURLcode err = curl_easy_perform(handle_);
    _CheckError(err, "easy perform");

    return write_stream_.str();
}

std::string Curl::
UrlEncode(const std::string& value) {
    char* encoded = curl_easy_escape(handle_, value.c_str(), (int)value.length());
    std::string retval (encoded);
    curl_free(encoded);
    return retval;
}

std::string Curl::
UrlDecode(const std::string& value) {
    int out_len;
    char* decoded = curl_easy_unescape(handle_,
            value.c_str(), (int) value.length(), &out_len);
    std::string retval (decoded, size_t(out_len));
    curl_free(decoded);
    return retval;
}

void Curl::
EnableHeader(bool onOff) {
    enable_header_ = onOff;
}

std::string Curl::
GetHeader() {
    return header_stream_.str();
}

size_t Curl::
WriteCb(void* buffer_p, size_t size, size_t nmemb) throw() {
    write_stream_ << std::string ((char*) buffer_p, size * nmemb);
    if (write_stream_.fail())
        return 0;
    return size * nmemb;
}

size_t Curl::
HeaderCb(void* buffer_p, size_t size, size_t nmemb) throw() {
    header_stream_ << std::string ((char*) buffer_p, size * nmemb);
    if (header_stream_.fail())
        return 0;
    return size * nmemb;
}

inline void Curl::
_CheckError(CURLcode err, const std::string& msg) {
    if (err != CURLE_OK) {
        throw CurlException(err, std::string("Failed ") + msg);
    }
}

void Curl::
_ResetHandle() {
    curl_easy_reset(handle_);
#ifdef  DEBUG
    curl_easy_setopt(handle_, CURLOPT_VERBOSE, 1L);
#ifdef  CRAZY_VERBOSE
    curl_easy_setopt(handle_, CURLOPT_DEBUGFUNCTION, _CurlTrace);
#endif
#endif
}

void Curl::
_SetCommonOptions(const std::string& url) {

    // set url
    CURLcode err = curl_easy_setopt(handle_, CURLOPT_URL, url.c_str());
    _CheckError(err, "set url");

    // Allow redirection
    err = curl_easy_setopt(handle_, CURLOPT_FOLLOWLOCATION, 1L);
    _CheckError(err, "set follow location");

    // Clear write stream
    write_stream_.str("");
    write_stream_.clear();

    // Set callback for write
    err = curl_easy_setopt(handle_, CURLOPT_WRITEFUNCTION, _WriteCb);
    _CheckError(err, "set write callback");

    // Set callback data
    err = curl_easy_setopt(handle_, CURLOPT_WRITEDATA, this);
    _CheckError(err, "set write data");

    if (enable_header_) {
        // Get curl header
        
        // clear existing header data
        header_stream_.str("");
        header_stream_.clear();

        // Set header callback function
        err = curl_easy_setopt(handle_, CURLOPT_HEADERFUNCTION, _HeaderCb);
        _CheckError(err, "set header callback");

        // Set header user data for callback function
        err = curl_easy_setopt(handle_, CURLOPT_HEADERDATA, this);
        _CheckError(err, "set header data");
    }
 
    // Set the user agent. Some servers requires this on requests
    err = curl_easy_setopt(handle_, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    _CheckError(err, "set write data");
}

void Curl::
_SetGetOptions(const std::string& url) {
    _SetCommonOptions(url);
}

void Curl::
_SetPostOptions(
    const std::string& url,
    const std::string& type,
    const CurlOptions& options) {

    CURLcode err;
    err = curl_easy_setopt(handle_, CURLOPT_CUSTOMREQUEST, type.c_str());
    _CheckError(err, "set request type");

    _SetCommonOptions(url);

    err = curl_easy_setopt(handle_, CURLOPT_POSTREDIR, CURL_REDIR_POST_ALL);
    _CheckError(err, "set post redir");

    std::ostringstream ostr;
    for (auto const& opt :options) {
        ostr << opt.first << '=' << opt.second << ';';
    }

    std::string opts (ostr.str());
    if (! opts.empty()) {
        err = curl_easy_setopt(handle_, CURLOPT_POST, 1L);
        _CheckError(err, "set post");
        err = curl_easy_setopt(handle_, CURLOPT_COPYPOSTFIELDS, opts.c_str());
        _CheckError(err, "set copy post fields");
    }
}

extern "C"
size_t _WriteCb(void* buffer_p, size_t size, size_t nmemb, internal::Curl* curl_p) {
    return curl_p->WriteCb(buffer_p, size, nmemb);
}

extern "C"
size_t _HeaderCb(void* buffer_p, size_t size, size_t nmemb, internal::Curl* curl_p) {
    return curl_p->HeaderCb(buffer_p, size, nmemb);
}


}
}
