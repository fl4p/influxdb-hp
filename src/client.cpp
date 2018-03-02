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
		batchTime = 24h;
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
		auto aMinuteAgo = system_clock::now() - 60s;
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

			if (bt1 >= aMinuteAgo)// fix: don't pollute cache with results from queries to futures (or near past)
				eo += "/*future!" + std::to_string(duration_cast<milliseconds>(aMinuteAgo.time_since_epoch()).count()) +
					  "*/";

			replace(bsql,
					":time_condition:",
					"(time >= '" + to8601(bt0) + "' AND time " + eo + " '" + to8601(bt1) + "')"
			);

			futs.emplace_back(queryRaw(bsql, [&columns, &results, bi](const char *body, size_t len) {
				rapidjson::Reader reader;

				if (columns.size() == 0) {
					ColumnReader colsReader;
					{
						rapidjson::StringStream ss(body);
						reader.Parse(ss, colsReader);
					}
					columns = colsReader.columns;
					//if(result.columns.size() == 0) throw std::runtime_error("empty columns array");
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

					if (result.num != result.time.size()) {
						throw std::runtime_error("unexpected time len");
					}
				}
			}));
		}

		for (auto &fut:futs) fut.wait();

		// sorted merge
		std::sort(results.begin(), results.end(), [](const fetchResult &a, const fetchResult &b) {
			if (a.time.empty() && b.time.empty()) return false;
			if (a.time.empty()) return true;
			if (b.time.empty()) return false;
			return a.time[0] < b.time[0];
		});

		fetchResult resultMerged;
		for (auto &r : results) resultMerged.num += r.num;

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
		auto req = new evpp::httpc::GetRequest(pool.get(), t->loop(), path);
		req->set_retry_interval(evpp::Duration(2.0));
		req->set_retry_number(10);

		std::shared_ptr<t_promise> result_promise = std::make_shared<t_promise>();
		req->Execute([result_promise, callback, req, sql](const t_resp &response) {
			auto hc = response->http_code();
			try {
				if (hc != 200) {
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
			delete req;
		});

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
		queryRaw(sqlFilled, [&](const char *body, size_t len) { 
			d.Parse(body, len);
		}).get();
		throwQueryError(d, sqlFilled);
		return d;
	}
}
