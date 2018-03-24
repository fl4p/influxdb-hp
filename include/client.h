#pragma  once

#include <memory>
#include <functional>
#include <future>
#include <vector>
#include <set>

#include <rapidjson/document.h>

#include "fetch.h"

namespace evpp {
    class EventLoopThread;
    namespace httpc {
        class ConnPool;

        class Response;

        class GetRequest;
    }
}

namespace influxdb {
    class client {
        std::unique_ptr<evpp::EventLoopThread> t;
        std::unique_ptr<evpp::httpc::ConnPool> pool;
        std::string dbName;
        std::chrono::milliseconds batchTime;

    public:
        client(const std::string &host, int port, const std::string &dbName);
        ~client();


        typedef influxdb::fetchResult fetchResult;


        /**
         * Fetches points for given time range of a single series using batched, async IO requests.
         * Just put `:time_condition:` in the WHERE clause.
         * @param sql
         * @param timeRange time interval, inclusive, ISO strings
         * @param args
         * @return
         */
        fetchResult
        fetch(const std::string &sql, std::array<std::string, 2> timeRange, const std::vector<std::string> &&args);

        std::set<std::string> queryTags(const std::string &sql, const std::vector<std::string> &&args = {});
        rapidjson::Document query(const std::string &sql, const std::vector<std::string> &&args = {});

        template<std::size_t N>
        std::set<std::string> queryTags(const std::string &sql, const std::array<std::string, N> &args) {
            return queryTags(sql, {args.begin(), args.end()});
        }

        std::future<void> queryRaw(const std::string &sql, std::function<void(const char *, size_t)> &&callback);
    };
};
