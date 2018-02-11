
#include <memory>
#include <functional>
#include <future>
#include <vector>

namespace evpp {
    class EventLoopThread;
    namespace httpc { class ConnPool; class Response; class GetRequest; }
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

        struct fetchResult {
            std::vector<std::string> columns;
            std::vector<float> data;
            int num = 0;
        };

        fetchResult fetch(const std::string &sql, const std::array<std::string,2> timeRange, const std::vector<std::string> &&args);

        std::vector<std::string> queryTags(const std::string &sql, const std::vector<std::string> &&args = {});
        void  query(const std::string &sql);

        std::future<void> queryRaw(const std::string &sql, std::function<void(const char *, size_t)> &&callback);
        std::future<const char*> queryRaw(const std::string &sql);

        static void
        HandleHTTPResponse(const std::shared_ptr<evpp::httpc::Response> &response, evpp::httpc::GetRequest *request,
                           std::promise<const char* >& promise);
    };
};
