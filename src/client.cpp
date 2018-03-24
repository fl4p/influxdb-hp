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

void throwQueryError(Document &d, const std::string &sql) {
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
    std::string sql;
    evpp::httpc::Request req;
    std::promise<void> promise;
    std::function<void(const char *, size_t)> callback;
    int retry;
};

void queryResultHandler(const t_resp &response, queryHandlerArgs *args) {
    auto hc = response->http_code();
    try {
        if (hc != 200) {
            if (args->retry < 7) {
                std::cerr << "influxdb http error, retry " << args->retry << std::endl;
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
        args->promise.set_value();
    } catch (...) {
        args->promise.set_exception(std::current_exception());
    }
    delete args;
};


namespace influxdb {
    std::string sqlArgs(std::string sql, const std::vector<std::string> &args) {
        for (auto &arg : args) util::replace(sql, "?", "'" + arg + "'");
        return sql;
    }

    client::client(const std::string &host, int port, const std::string &dbName) : dbName(dbName) {
        wsaStart();
        pool = std::make_unique<evpp::httpc::ConnPool>(host, port, evpp::Duration(30.0));
        t = std::make_unique<evpp::EventLoopThread>();
        t->Start(true);
        batchTime = 48h;
    }

    client::~client() {
        pool->Clear();
        t->Stop(true);
    }

    client::fetchResult
    client::fetch(const std::string &sql, std::array<std::string, 2> timeRange,
                  const std::vector<std::string> &&args) {
        using namespace std::chrono;
        using namespace std::chrono_literals;
        using namespace util;
        auto aMinAgo = time_point_cast<milliseconds>(system_clock::now() - 60s);
        if (timeRange[0].find('T') == std::string::npos) timeRange[0] += "T00:00:00.000Z";
        if (timeRange[1].find('T') == std::string::npos) timeRange[1] += "T00:00:00.000Z";
        auto t0 = util::parse8601(timeRange[0]), t1 = util::parse8601(timeRange[1]);
        size_t batches = (size_t) std::ceil(milliseconds(t1 - t0).count() / (float) milliseconds(batchTime).count());

        auto fsql = sqlArgs(sql, args);

        std::vector<std::future<void>> futs;
        std::vector<std::string> columns;
        std::vector<fetchResult> results{batches};

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
                    queryRaw(bsql, [&columns, &results, bi, bsql /*, &cache*/](const char *body, size_t len) {
                        rapidjson::Reader reader;

                        if (columns.size() == 0) {
                            ColumnReader colsReader;
                            {
                                rapidjson::StringStream ss(body);
                                reader.Parse(ss, colsReader);
                            }
                            columns = colsReader.columns;
                        }

                        if (columns.size() != 0) {
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

                            if (result.num != result.time.size()) {
                                throw std::runtime_error("unexpected time len");
                            }
                            //cache.set(bsql, result);
                        }
                    }));
        }

        for (auto &fut:futs) fut.get();

        // sorted merge
        std::sort(results.begin(), results.end(), [](const fetchResult &a, const fetchResult &b) {
            if (a.time.empty() && b.time.empty()) return false;
            if (a.time.empty()) return true;
            if (b.time.empty()) return false;
            return a.time[0] < b.time[0];
        });

        fetchResult resultMerged{0, 0, {}, {}, {}};
        if (results.empty()) return resultMerged;
        for (auto &r : results) resultMerged.num += r.num;
        if (columns.empty()) columns = results[0].columns;

        //LOG_W << LOG_EXPR(columns.size());

        resultMerged.dataStride = (columns.size() - 1);
        resultMerged.columns = columns;
        resultMerged.time.resize(resultMerged.num);
        resultMerged.data.resize(resultMerged.num * resultMerged.dataStride);

        size_t offset = 0;
        for (auto &r : results) {
            std::move(r.time.begin(), r.time.end(), resultMerged.time.begin() + offset);
            std::move(r.data.begin(), r.data.end(), resultMerged.data.begin() + (offset * resultMerged.dataStride));
            offset += r.num;
        }

        return resultMerged;
    }

    std::future<void> client::queryRaw(const std::string &sql, std::function<void(const char *, size_t)> &&callback) {
        typedef std::promise<void> t_promise;
        typedef std::shared_ptr<evpp::httpc::Response> t_resp;

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

        auto handlerArgs = new queryHandlerArgs{
                sql, evpp::httpc::GetRequest(pool.get(), t->loop(), path),
                {}, callback, 0,
        };
        handlerArgs->req.Execute(std::bind(queryResultHandler, std::placeholders::_1, handlerArgs));

        //req->Execute(handler);

        return std::move(handlerArgs->promise.get_future());
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
}
