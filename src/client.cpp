#if _WIN32
#define WIN32_LEAN_AND_MEAN
#endif

#include <evpp/event_loop_thread.h> // overrides errno!
#include <evpp/httpc/conn_pool.h>
#include <evpp/httpc/request.h>
#include <evpp/httpc/response.h>

#include <cmath>
#include <future>
#include <array>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "client.h"
#include "json-readers.h"
#include "util.h"
#include "cache.h"


date::sys_time<std::chrono::milliseconds>
batchTime0(date::sys_time<std::chrono::milliseconds> &&tp, std::chrono::milliseconds &batchTime) {
    using namespace std::chrono;
    auto epochMs = milliseconds(tp.time_since_epoch()).count();
    auto batchMs = milliseconds(batchTime).count();
    int64_t bt0Ms = static_cast<int64_t>((epochMs / (float) batchMs)) * batchMs;

    date::sys_time<std::chrono::milliseconds> tp_epoch;
    return tp_epoch + std::chrono::milliseconds(bt0Ms);

}

void throwQueryError(rapidjson::Document &d, const std::string &sql) {
    if (d.HasParseError()) {
        throw std::runtime_error("response parse error");
    }
    if (!d.HasMember("results")) {
        throw std::runtime_error("influxdb response has no results member, query was \"" + sql + "\"");
    }
    if (d["results"][0].HasMember("error")) {
        throw std::runtime_error(
                "influxdb error: " + std::string(d["results"][0]["error"].GetString()) + ", SQL \"" + sql + "\"");
    }
}


typedef std::shared_ptr<evpp::httpc::Response> t_resp;
struct queryHandlerArgs {
    std::atomic<int> &numPending;
    std::string sql;
    evpp::httpc::GetRequest req;
    std::shared_ptr<std::promise<void>> promise;
    std::function<void(const char *, size_t)> callback;
    int retry;
};

void queryResultHandler(const t_resp &response, queryHandlerArgs *args) {
    using namespace std::chrono_literals;

    --args->numPending;

    auto hc = response->http_code();
    try {
        if (hc != 200) {
            if (args->retry < 7) {
                std::cerr << "influxdb http error " << hc << " with query \"" << args->sql << "\" request http://"
                          << args->req.host() << ":"
                          << args->req.port() << args->req.uri() << ", retry " << args->retry << std::endl;
                std::this_thread::sleep_for(200ms * std::pow(2, args->retry++));
                args->req.Execute(std::bind(queryResultHandler, std::placeholders::_1, args));
                return;
            }
            std::cerr << "influxdb http error " << hc << ": " << response->body().ToString() << std::endl
                      << "Query: " << args->sql << std::endl;
            throw std::runtime_error(
                    "influxdb http error " + std::to_string(hc) + " " + response->body().ToString());
        }
        // auto date(util::parseHttpDate(response->FindHeader("Date")));
        // LOG_I << "server-data:" << util::to8601(date);
        args->callback(response->body().data(), response->body().size());
        args->promise->set_value();
    } catch (...) {
        args->promise->set_exception(std::current_exception());
    }
    delete args;
};


namespace influxdb {


    std::string sqlArgs(std::string sql, const std::vector<std::string> &args) {
        for (auto &arg : args) util::replace(sql, "?", "'" + arg + "'");
        return sql;
    }

    client::client(const std::string &host, int port, const std::string &dbName, std::chrono::milliseconds batchTime,
                   size_t connPoolSize) : dbName(dbName), connPoolSize(connPoolSize) {
        wsaStart();
        pool = std::make_unique<evpp::httpc::ConnPool>(host, port,
                                                       evpp::Duration(static_cast<double >(RequestTimeoutSeconds)),
                                                       connPoolSize);
        t = std::make_unique<evpp::EventLoopThread>();
        t->Start(true);
        this->batchTime = batchTime;
    }

    client::~client() {
        pool->Clear();
        t->Stop(true);
    }


    auto sortedMergeGrouped(const std::vector<std::vector<series>> &batches, const TagsKeyFunc &keyFunc)
    -> std::unordered_map<std::string, series> {

        std::unordered_map<std::string, std::vector<series>> grouped;
        for (auto &batch:batches) {
            for (auto &r:batch) {
                auto key = keyFunc(r.tags);
                auto f = grouped.find(key);
                if (f == grouped.end()) {
                    f = grouped.emplace(key, std::vector<series>{}).first;
                }
                f->second.emplace_back(std::move(r));
            }
        }

        std::unordered_map<std::string, series> merged;
        for (auto &kv:grouped) {
            merged.emplace(kv.first, series::sortedMerge(kv.second));
        }

        return merged;
    }

    void maybeFixTimeRange(std::array<std::string, 2> &timeRange) {
        if (timeRange[0].find('T') == std::string::npos) timeRange[0] += "T00:00:00.000Z";
        if (timeRange[1].find('T') == std::string::npos) timeRange[1] += "T00:00:00.000Z";
    }

    client::fetchResult
    client::fetch(const std::string &sql, std::array<std::string, 2> timeRange,
                  const std::vector<std::string> &&args) {
        using namespace std::chrono;
        using namespace std::chrono_literals;
        using namespace util;

        maybeFixTimeRange(timeRange);

        auto aMinAgo = time_point_cast<milliseconds>(system_clock::now() - 60s);

        auto t0 = util::parse8601(timeRange[0]), t1 = util::parse8601(timeRange[1]);
        size_t batches = (size_t) std::ceil(milliseconds(t1 - t0).count() / (float) milliseconds(batchTime).count());

        auto fsql = sqlArgs(sql, args);

        std::vector<std::future<void>> futs;
        std::vector<std::string> columns;
        std::vector<fetchResult> results{batches};

        std::mutex mtxColumns;

        for (int bi = 0; bi < batches; ++bi) {
            auto bsql = fsql;
            auto bt = batchTime0(t0 + batchTime * bi, batchTime);
            auto bt0 = (bi == 0) ? t0 : bt, bt1 = (bi == (batches - 1)) ? t1 : std::min({bt + batchTime, t1});

            std::string eo = bi < (batches - 1) ? "<" : "<=";

            if (bt1 >= aMinAgo)// fix: don't pollute cache with results from queries to futures (or near past)
                eo += "/*future!" + std::to_string(aMinAgo.time_since_epoch().count()) + "*/";

            replace(bsql, ":time_condition:",
                    "(time >= '" + to8601(bt0) + "' AND time " + eo + " '" + to8601(bt1) + "')"
            );

            // LOG_D << "f:" << LOG_EXPR(bsql);
            //file_cache<fetchResult> cache{"influx-cache"};
            futs.emplace_back(
                    //false && cache.have(bsql)
                    //? cache.get_async_throw(bsql, results[bi])                  :
                    queryRaw(bsql,
                             [&mtxColumns, &columns, &results, bi, bsql /*, &cache*/]
                                     (const char *body, size_t len) {
                                 rapidjson::Reader reader;

                                 //LOG_D << "body:" << std::string(body);

                                 {
                                     std::lock_guard<std::mutex> lg{mtxColumns};
                                     if (columns.size() == 0) {
                                         ColumnReader colsReader;
                                         {
                                             rapidjson::StringStream ss(body);
                                             reader.Parse(ss, colsReader);
                                         }
                                         columns = colsReader.columns;
                                     }
                                 }


                                 auto &result(results[bi]);
                                 DataReader dataReader{columns.size(), result};
                                 rapidjson::StringStream ss(body);
                                 reader.Parse(ss, dataReader);
                                 if (result.data.size() % (columns.size() - 1)) {
                                     throw std::runtime_error("unexpected data len");
                                 }

                                 result.dataStride = (columns.size() - 1);
                                 result.num = result.data.size() / result.dataStride;
                                 result.columns = columns; // copy for cache

                                 result.checkNum();
                                 //cache.set(bsql, result);

                             }));
        }


        std::exception_ptr firstException = nullptr;
        for (auto &fut:futs) {
            try {
                fut.get();
            } catch (const std::runtime_error &re) {
                if (!firstException) {
                    LOG_E << "fetch error: " << re.what();
                    firstException = std::current_exception();
                }
            } catch (...) {
                if (!firstException) {
                    firstException = std::current_exception();
                }
            }
        }

        if (firstException) {
            std::rethrow_exception(firstException);
        }

        return series::sortedMerge(results);
    }

    std::future<void> client::queryRaw(const std::string &sql, std::function<void(const char *, size_t)> &&callback) {
        typedef std::promise<void> t_promise;
        typedef std::shared_ptr<evpp::httpc::Response> t_resp;

        std::shared_ptr<t_promise> result_promise = std::make_shared<t_promise>();

        //LOG_D << sql;
        //std::cout << sql << std::endl;
        auto path = "/query?db=" + dbName + "&epoch=ms&q=" + util::urlEncode(sql);

        /*auto req = new evpp::httpc::GetRequest(pool.get(), t->loop(), path);
        //auto rh = new retryHandler{0, nullptr};
        req->set_retry_interval(evpp::Duration(2.0));
        req->set_retry_number(10);

        std::shared_ptr<t_promise> result_promise = std::make_shared<t_promise>();
        auto handler = [result_promise, callback, req, sql](const t_resp &response) {
            auto hc = response->http_code();
            try {
                if (hc != 200) {
                   /* if (rh->retry < 5) {
                        std::cerr << "influxdb http error, retry " << rh->retry<< std::endl;
                        std::this_thread::sleep_for(200ms * std::pow(2, rh->retry++));
                        req->Execute(rh->handler);
                        return;
                     }* /
                    std::cerr << "influxdb http error " << hc << ": " << response->body().ToString() << std::endl
                              << "Query: " << sql << std::endl;
                    throw std::runtime_error(
                            "influxdb http error " + std::to_string(hc) + " " + response->body().ToString());
                }
                // auto date(util::parseHttpDate(response->FindHeader("Date")));
                // LOG_I << "server-data:" << util::to8601(date);
                callback(response->body().data(), response->body().size());
                result_promise->set_value();
            } catch (...) {
                result_promise->set_exception(std::current_exception());
            }
            delete req; //delete rh;
        };
         */

        if (numPendingReq >= connPoolSize) {
            //LOG_W << LOG_EXPR(numPendingReq);
            // TODO std::condition_variable cv; cv.wait(lk, []{return ready;});
            while (numPendingReq > connPoolSize / 2) {
                std::this_thread::sleep_for(0.1ms + 0.01ms * (std::rand() % 100));
            }
        }

        ++numPendingReq;
        auto handlerArgs = new queryHandlerArgs{
                numPendingReq,
                sql,
                evpp::httpc::GetRequest{pool.get(), t->loop(), path},
                result_promise, callback, 0,};
        handlerArgs->req.Execute(std::bind(queryResultHandler, std::placeholders::_1, handlerArgs));

        //req->Execute(handler);

        return std::move(result_promise->get_future());
    }

    static std::string jsonToString(const rapidjson::Value &jv) {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        jv.Accept(writer);
        return buffer.GetString();
    }

    std::set<std::string> client::queryTags(const std::string &sql, const std::vector<std::string> &&args) {
        Document d = query(sql, std::move(args));
        if (!d["results"][0].HasMember("series")) return {};
        std::set<std::string> tags;
        for (auto &s : d["results"][0]["series"].GetArray()) {
            tags.insert(s["tags"].MemberBegin()->value.GetString());
        }
        return tags;
    }

    rapidjson::Document client::query(const std::string &sql, const std::vector<std::string> &&args) {
        auto sqlFilled = sqlArgs(sql, args);
        Document d;
        for (int i = 0; i < 4; ++i) {
            queryRaw(sqlFilled, [&](const char *body, size_t len) {
                d.Parse(body, len);
            }).get();
            if (!d.HasParseError()) break;
            std::cerr << "query result parse error, retry " << (i + 1) << std::endl;
            std::this_thread::sleep_for(200ms * std::pow(2, i));
        }
        throwQueryError(d, sqlFilled);
        return d;
    }


    std::unordered_map<std::string, series>
    client::fetchGroups(const std::string &sql, std::array<std::string, 2> timeRange,
                        const std::vector<std::string> &&args,
                        const TagsKeyFunc &keyFunc) {
        using namespace std::chrono;
        using namespace std::chrono_literals;
        using namespace util;

        maybeFixTimeRange(timeRange);

        auto aMinAgo = time_point_cast<milliseconds>(system_clock::now() - 60s);

        auto t0 = util::parse8601(timeRange[0]), t1 = util::parse8601(timeRange[1]);
        size_t numBatches = (size_t) std::ceil(milliseconds(t1 - t0).count() / (float) milliseconds(batchTime).count());

        auto fsql = sqlArgs(sql, args);

        std::vector<std::future<void>> futs;
        std::vector<std::string> columns;
        std::vector<std::vector<series>> batches{numBatches};

        for (int bi = 0; bi < numBatches; ++bi) {
            auto bsql = fsql;
            auto bt = batchTime0(t0 + batchTime * bi, batchTime);
            auto bt0 = (bi == 0) ? t0 : bt, bt1 = (bi == (numBatches - 1)) ? t1 : std::min({bt + batchTime, t1});

            std::string eo = bi < (numBatches - 1) ? "<" : "<=";

            if (bt1 >= aMinAgo)// fix: don't pollute cache with results from queries to futures (or near past)
                eo += "/*future!" + std::to_string(aMinAgo.time_since_epoch().count()) + "*/";

            replace(bsql, ":time_condition:",
                    "(time >= '" + to8601(bt0) + "' AND time " + eo + " '" + to8601(bt1) + "')"
            );

            // LOG_D << "f:" << LOG_EXPR(bsql);
            //file_cache<fetchResult> cache{"influx-cache"};
            futs.emplace_back(
                    //false && cache.have(bsql)
                    //? cache.get_async_throw(bsql, results[bi])                  :
                    queryRaw(bsql, [&columns, &batches, bi, bsql /*, &cache*/](const char *body, size_t len) {
                        rapidjson::Reader reader;

                        //  LOG_D << bsql << " resp body len=" << len;

                        throw "fix race condition!";
                        if (columns.size() == 0) {
                            ColumnReader colsReader;
                            {
                                rapidjson::StringStream ss(body);
                                reader.Parse(ss, colsReader);
                            }
                            columns = colsReader.columns;
                        }

                        if (columns.size() != 0) {
                            auto &batch(batches[bi]);
                            SeriesReader dataReader{columns.size(), batch};
                            rapidjson::StringStream ss(body);
                            reader.Parse(ss, dataReader);
                            for (auto &r:batch) {
                                if (r.data.size() % (columns.size() - 1)) {
                                    throw std::runtime_error("unexpected data len");
                                }
                                r.dataStride = (columns.size() - 1);
                                r.num = r.data.size() / r.dataStride;
                                r.columns = columns; // copy for cache

                                //if (r.num != r.time.size()) { // TODO
                                //    throw std::runtime_error("unexpected time len");
                                //}
                            }

                            //cache.set(bsql, result);
                        }
                    }));
        }

        for (auto &fut:futs) fut.get();

        throw std::logic_error("need exception handling, see fetch()");

        return sortedMergeGrouped(batches, keyFunc);
    }
}
