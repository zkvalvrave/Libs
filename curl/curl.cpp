#include "curl.h"



#define DEFAULT_CURL_TIMEOUT        10        // s
#define DEFAULT_IMAGE_MAX_SIZE            (10 * 1024 * 1024) // 10M
static size_t write_data(void *buff, size_t size, size_t nmemb, void *userdata)
{
    assert(userdata != NULL);

    std::string* str = dynamic_cast<std::string*>((std::string*)userdata);
    try{
        str->append((char*)buff, size * nmemb);
    }catch(...)
    {
        return 0;
    }
    return size * nmemb;
}

bool CCurl::DownloadImage(const std::string &url, std::string& data)
{
    data.clear();
    CURLcode res = CURLE_FAILED_INIT;

    do{
        if(!curl)
        {
            curl = curl_easy_init();
            if(curl)
            {
                curl_slist *http_headers = NULL;
                http_headers = curl_slist_append(http_headers, "Connection: keep-alive");
                http_headers = curl_slist_append(http_headers, "Cache-Control: no-cache");
                http_headers = curl_slist_append(http_headers, "Pragma: no-cache");
                curl_easy_setopt(curl, CURLOPT_HTTPHEADER, http_headers);

                curl_easy_setopt(curl, CURLOPT_AUTOREFERER, 1);
                curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
                curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 1);
                curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
            }
        }
        if (!curl)     break;

        res = curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        if (res != CURLE_OK) break;

        res = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
        if (res != CURLE_OK) break;

        res = curl_easy_setopt(curl, CURLOPT_WRITEDATA, &data);
        if (res != CURLE_OK) break;

        res = curl_easy_setopt(curl, CURLOPT_TIMEOUT, DEFAULT_CURL_TIMEOUT);
        if (res != CURLE_OK) break;

        res = curl_easy_perform(curl);

    }while(0);
     
    return res == CURLE_OK ? true : false;
}

CCurl::CCurl()
{
    curl = NULL;
}

CCurl::~CCurl()
{
    if(!curl)
        curl_easy_cleanup(curl);

    curl = NULL;
}
