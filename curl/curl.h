#ifndef _CCURL_H_
#define _CCURL_H_

#include <string>
#include <assert.h>
#include <curl/curl.h>

/*************************************************
Copyright:sensenets
Author: zengkun
Date:2018-12-27
Description: curl 操作对象, 根据url 下载指定的资源
**************************************************/
class CCurl
{
public:
    CCurl();
    ~CCurl();
    bool DownloadImage(const std::string&url, std::string& data);
    CURL* GetCurl();

private:
    CURL *curl;
};

typedef CCurl* CURL_HANDLE;

#endif
