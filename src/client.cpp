#if _WIN32
#define WIN32_LEAN_AND_MEAN
#endif

#include <evpp/event_loop_thread.h>

#include <evpp/httpc/conn_pool.h>
#include <evpp/httpc/request.h>
#include <evpp/httpc/conn.h>
#include <evpp/httpc/response.h>
#include <cmath>
#include <future>

#include "client.h"
#include "util.h"
#include "json-readers.h"
#include "../../pclog/pclog.h"
#include <future>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>


date::sys_time<std::chrono::milliseconds>
batchTime0(date::sys_time<std::chrono::milliseconds> &&tp, std::chrono::milliseconds &batchTime) {
    using namespace std::chrono;
    auto epochMs = milliseconds(tp.time_since_epoch()).count();
    auto batchMs = milliseconds(batchTime).count();
    int64_t bt0Ms = static_cast<int64_t>((epochMs / (float) batchMs)) * batchMs;

    date::sys_time<std::chrono::milliseconds> tp_epoch;
    return tp_epoch + std::chrono::milliseconds(bt0Ms);

}


namespace influxdb {
    std::string sqlArgs(std::string sql, const std::vector<std::string> &args) {
        for (auto &arg : args) {
            util::replace(sql, "?", "'" + arg + "'");
        }
        return sql;
    }

    client::client(const std::string &host, int port, const std::string &dbName) : dbName(dbName) {
        wsaStart();
        pool = std::make_unique<evpp::httpc::ConnPool>(host, port, evpp::Duration(30.0));
        t = std::make_unique<evpp::EventLoopThread>();
        t->Start(true);
        batchTime = 24h;
    }

    client::~client() {
        pool->Clear();
        t->Stop(true);
    }


    client::fetchResult client::fetch(const std::string &sql, const std::array<std::string, 2> timeRange,
                                      const std::vector<std::string> &&args) {
        using namespace std::chrono;
        using namespace util;
        auto t0 = util::parse8601(timeRange[0]), t1 = util::parse8601(timeRange[1]);
        size_t batches = (size_t) std::ceil(milliseconds(t1 - t0).count() / (float) milliseconds(batchTime).count());

        auto fsql = sqlArgs(sql, args);

        std::vector<std::future<void>> futs;
        std::vector<std::string> columns;
        std::vector<fetchResult> results{batches};
        //fetchResult result;

        for (int bi = 0; bi < batches; ++bi) {
            auto bsql = fsql;
            auto bt = batchTime0(t0 + batchTime * bi, batchTime);
            auto bt0 = (bi == 0) ? t0 : bt, bt1 = std::min({bt + batchTime, t1});
            auto eo = bi < (batches - 1) ? "<" : "<=";
            replace(bsql, ":time_condition:",
                    "time >= '" + to8601(bt0) + "' AND time " + eo + " '" + to8601(bt1) + "'");

            // with batching responses can arrive out-of-order!
            futs.emplace_back(queryRaw(bsql, [&columns, &results, bi](const char *body, size_t len) {
                rapidjson::Reader reader;

                if (columns.size() == 0) {
                    // capture columns
                    ColumnReader colsReader;
                    {
                        rapidjson::StringStream ss(body);
                        reader.Parse(ss, colsReader);
                    }
                    columns = colsReader.columns;
                    //if(result.columns.size() == 0)
                    //    throw std::runtime_error("empty columns array");
                }

                if (columns.size() != 0) {
                    auto &result(results[bi]);
                    DataReader dataReader{columns.size(), result};
                    rapidjson::StringStream ss(body);
                    reader.Parse(ss, dataReader);
                    if (result.data.size() % (columns.size() - 1)) {
                        throw std::runtime_error("unexpected data len");
                    }
                    result.num = result.data.size() / (columns.size() - 1);

                    if (result.num != result.time.size()) {
                        throw std::runtime_error("unexpected time len");
                    }
                }
            }));
        }

        for (auto &fut:futs) {
            fut.wait();
        }

        // merge
        std::sort(results.begin(), results.end(), [](const fetchResult &a, const fetchResult &b) {
            if (a.time.empty()) return true;
            if (b.time.empty()) return false;
            return a.time[0] < b.time[0];
        });

        fetchResult resultMerged;
        for (auto &r : results) resultMerged.num += r.num;

        size_t numDataCols = (columns.size() - 1);
        resultMerged.columns = columns;
        resultMerged.time.resize(resultMerged.num);
        resultMerged.data.resize(resultMerged.num * numDataCols);

        size_t offset = 0;
        for (auto &r : results) {
            std::move(r.time.begin(), r.time.end(), resultMerged.time.begin() + offset);
            std::move(r.data.begin(), r.data.end(), resultMerged.data.begin() + (offset * numDataCols));
            offset += r.num;
        }

        return resultMerged;
    }


    std::future<const char *> client::queryRaw(const std::string &sql) {
        // std::cout << sql << std::endl;
        auto path = "/query?pretty=false&db=" + dbName + "&epoch=ms&q=" + util::urlEncode(sql);
        auto *r = new evpp::httpc::GetRequest(pool.get(), t.get()->loop(), path);

        typedef std::promise<const char *> t_promise;
        typedef std::shared_ptr<evpp::httpc::Response> t_resp;
        std::shared_ptr<t_promise> result_promise = std::make_shared<t_promise>();
        // r->Execute(std::bind(&HandleHTTPResponse, std::placeholders::_1, r, (result_promise)));

        r->Execute(std::function<void(const t_resp &response)>([result_promise](const t_resp &response) {
            auto hc = response->http_code();

            if (hc != 200) {
                throw std::runtime_error("http error " + std::to_string(hc));
            }

            try {
                result_promise->set_value(response->body().data());
            } catch (...) {
                result_promise->set_exception(std::current_exception());
            }
        }));
        return result_promise->get_future();
    }

    std::future<void> client::queryRaw(const std::string &sql, std::function<void(const char *, size_t)> &&callback) {
        // std::cout << sql << std::endl;
        auto path = "/query?pretty=false&db=" + dbName + "&epoch=ms&q=" + util::urlEncode(sql);
        auto *r = new evpp::httpc::GetRequest(pool.get(), t->loop(), path);

        typedef std::promise<void> t_promise;
        typedef std::shared_ptr<evpp::httpc::Response> t_resp;

        std::shared_ptr<t_promise> result_promise = std::make_shared<t_promise>();

        r->set_retry_interval(evpp::Duration(2.0));
        r->set_retry_number(10);
        r->Execute([result_promise, callback](const t_resp &response) {
            auto hc = response->http_code();
            try {
                if (hc != 200) {
                    std::cerr << "http error " << hc << std::endl;

                    // r->Execute(std::bind(&HandleHTTPResponse, std::placeholders::_1, req));
                    throw std::runtime_error("http error " + std::to_string(hc) + " " + response->body().ToString());
                }
                callback(response->body().data(), response->body().size());
                result_promise->set_value();
            } catch (...) {
                result_promise->set_exception(std::current_exception());
            }
        });

        return std::move(result_promise->get_future());
    }


    std::vector<std::string> client::queryTags(const std::string &sql, const std::vector<std::string> &&args) {
        Document d;
        queryRaw(sqlArgs(sql, args), [&](const char *body, size_t len) {
            if (d.Parse(body, len).HasParseError()) {
                std::cerr << "json parse error" << std::endl;
            }
        }).wait();

        auto &series = d["results"][0]["series"];
        std::vector<std::string> tags;
        for (int i = 0; i < series.Size(); i++) {
            tags.emplace_back(series[i]["tags"].MemberBegin()->value.GetString());
        }

        return tags;
    }

    void
    client::HandleHTTPResponse(const std::shared_ptr<evpp::httpc::Response> &response,
                               evpp::httpc::GetRequest *request, std::promise<const char *> &promise) {
        auto hc = response->http_code();


        try {
            promise.set_value(response->body().data());
        } catch (...) {
            promise.set_exception(std::current_exception());
        }


        auto b = response->body().ToString();
        // LOG_INFO << "http_code=" << response->http_code() << " [" << response->body().ToString() << "]";
        std::string header = response->FindHeader("Connection");
        // LOG_INFO << "HTTP HEADER Connection=" << header;

        // retry:
        //  req->Execute(std::bind(&HandleHTTPResponse, std::placeholders::_1, req));
        //responsed = true;
        assert(request == response->request());
        delete request; // The request MUST BE deleted in EventLoop thread.
    }
}

