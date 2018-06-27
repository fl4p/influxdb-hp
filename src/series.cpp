#include <algorithm>
#include <cmath>
#include <functional>
#include "series.h"

namespace influxdb {
    void series::joinInner(const series &other) {
        size_t selfA = 0, otherA = 0;
        while (otherA < other.num && other.t(otherA) < t(0)) ++otherA;
        if (otherA == other.num) throw std::runtime_error("cannot join empty or non-overlapping series!");
        while (other.t(otherA) != t(selfA)) {
            ++selfA;
            if (selfA >= num) throw std::runtime_error("cannot join series with different sampling interval");
        }

        std::vector<float> joint;
        joint.reserve(data.size() + other.data.size());

        size_t k = 0;
        for (k = 0; k + selfA < num && k + otherA < other.num; ++k) {
            if (t(k + selfA) != other.t(k + otherA))
                throw std::runtime_error("cannot join series with various sampling intervals");
            std::copy(data.begin() + (selfA + k) * dataStride, data.begin() + (selfA + k + 1) * dataStride,
                      std::back_inserter(joint));
            std::copy(other.data.begin() + (otherA + k) * other.dataStride,
                      other.data.begin() + (otherA + k + 1) * other.dataStride, std::back_inserter(joint));
        }

        joint.shrink_to_fit();

        num = k;
        std::copy(other.columns.begin() + 1, other.columns.end(), std::back_inserter(columns));
        dataStride += other.dataStride;
        data = std::move(joint);

        // TODO time compacted!
        time.erase(time.begin(), time.begin() + selfA);
        time.erase(time.begin() + num, time.end());
    }


    series series::sortedMerge(std::vector<fetchResult> &results) {

        // remove empty series
        results.erase(std::remove_if(results.begin(), results.end(), [](const fetchResult &r) {
            return r.num == 0;
        }), results.end());

        fetchResult resultMerged{};
        if (results.empty())
            return resultMerged;

        std::sort(results.begin(), results.end(), [](const fetchResult &a, const fetchResult &b) {
            return a.t(0) < b.t(0);
        });

        // LOG_D << LOG_EXPR(results.size());
        // strip overlaps TODO
        for (auto i = 0; (i + 1) < results.size(); ++i) {
            if (results[i].tEnd() >= results[i + 1].t(0)) {
                LOG_D << LOG_EXPR(i) << LOG_EXPR(results[i].tEnd()) << LOG_EXPR(results[i + 1].t(0))
                      << LOG_EXPR(results[i].tEnd() - results[i + 1].t(0));
                throw std::logic_error("cant merge time-overlapping results!");
            }
        }


        for (auto &r : results)
            resultMerged.num += r.num;
        auto columns = results[0].columns;

        if (columns.empty()) throw std::runtime_error("sortedMerge: no columns!");

        //LOG_W << LOG_EXPR(columns.size());
        resultMerged.dataStride = (columns.size() - 1);
        resultMerged.columns = columns;
        resultMerged.time.resize(resultMerged.num);
        // LOG_W << LOG_EXPR(columns.size()) << LOG_EXPR(resultMerged.num) << LOG_EXPR(resultMerged.dataStride);
        resultMerged.data.resize(resultMerged.num * resultMerged.dataStride);

        size_t offset = 0;
        for (auto &r : results) {
            std::move(r.time.begin(), r.time.end(), resultMerged.time.begin() + offset);
            std::move(r.data.begin(), r.data.end(), resultMerged.data.begin() + (offset * resultMerged.dataStride));
            offset += r.num;
        }

        /*
        //for(auto &r: results) {
        uint64_t lastT = 0, i = 0;
        for (auto t:resultMerged.time) {
            if (t < lastT) {
                LOG_W << i << " " << LOG_EXPR(t) << LOG_EXPR(lastT) << LOG_EXPR((lastT - t) / 1e3);
            }
            lastT = t;
            ++i;
        }*/
        // }

        // fill NaNs with previous
        for (size_t i = 1; i < resultMerged.num; ++i) {
            for (size_t c = 0; c < resultMerged.dataStride; ++c) {
                if (std::isnan(resultMerged.data[i * resultMerged.dataStride + c])) {
                    resultMerged.data[i * resultMerged.dataStride + c] = resultMerged.data[
                            (i - 1) * resultMerged.dataStride + c];
                }
            }
        }

        return resultMerged;
    }


    size_t series::trim() {
        return trim([](const float *d, size_t len) {
            for (size_t c = 0; c < len; ++c) {
                if (std::isnan(d[c])) return false;
            }
        });
    }

    void series::erase(size_t start, size_t count) {
        //LOG_D << __FUNCTION__ << LOG_EXPR(start) << LOG_EXPR(count);

        if (start + count > num) throw std::logic_error("erase: out of range");
        num -= count;
        data.erase(data.begin() + start, data.begin() + (start + count) * dataStride);
        time.erase(time.begin() + start, time.begin() + (start + count));
        checkNum();
    }

    decltype(series::time.begin()) series::insert(size_t start, size_t count) {
        //LOG_D << __FUNCTION__ << LOG_EXPR(start) << LOG_EXPR(count);

        //if(start < 2) throw std::logic_error("insert: can only insert after first 2 samples");
        //if(num < 2) throw std::logic_error("insert: series must have at least 2 samples");
        num += count;
        data.insert(data.begin() + start, dataStride * count, 0);
        time.insert(time.begin() + start, count, 0); // todo time compact

        checkNum();

        return time.begin() + start;
        //auto si = t(1) - t(0);
        //for (size_t i = 0; i < count; ++i)
        //    time[start + i] =  i * si;
    }

    size_t series::fill(const std::function<bool(const float *, size_t)> &pred) {
        if (num < 2) return 0;

        size_t filled = 0;

        // fill invalid with previous
        for (size_t i = 1; i < num; ++i) {
            if (!pred(&data[i * dataStride], dataStride)) {
                for (size_t c = 0; c < dataStride; ++c) {
                    data[i * dataStride + c] = data[(i - 1) * dataStride + c];
                    ++filled;
                }
            }
        }

        filled += fillTimeGaps();

        return filled;
    }

    size_t series::fill() {
        if (num < 2) return 0;

        size_t filled = 0;

        // fill NaNs with previous
        for (size_t i = 1; i < num; ++i) {
            for (size_t c = 0; c < dataStride; ++c) {
                if (std::isnan(data[i * dataStride + c])) {
                    data[i * dataStride + c] = data[(i - 1) * dataStride + c];
                    ++filled;
                }
            }
        }

        filled += fillTimeGaps();

        return filled;
    }

    size_t series::fillTimeGaps() {
        size_t filled = 0;
        auto si = t(1) - t(0);
        auto lastT = t(0);
        for (size_t i = 1; i < num; ++i) {
            auto nIns = (t(i) - lastT) / si - 1;
            if (nIns != 0) {
                if (nIns < 0) throw std::runtime_error("unexpected time jump backwards");
                // insert repeating previous
                time.insert(time.begin() + i, static_cast<size_t>(nIns), 0);
                data.insert(data.begin() + i * dataStride, nIns * dataStride, 0.f);
                for (size_t j = 0; j < nIns; ++j) {
                    time[i + j] = lastT + (1 + j) * si;
                    for (size_t c = 0; c < dataStride; ++c) {
                        data[(i + j) * dataStride + c] = data[(i - 1) * dataStride + c];
                    }
                }
                num += nIns;
                i += nIns;
                filled += nIns;
            }
            lastT = t(i);
        }
        return filled;
    }

    /*
    void series::equalStartTimes(const std::vector<std::reference_wrapper<series>> &series_, int64_t t) {
        // slice start
        for (auto &sr:series_) {
            series &s{sr.get()};
            if (s.t(0) < t) {
                size_t i = 0;
                while (i < s.time.size() && s.t(i) < t) { ++i; }
                if (i >= s.num) {
                    s.num = 0;
                    s.data.clear();
                    s.time.clear();
                } else {
                    s.num -= i;
                    s.data.erase(s.data.begin(), s.data.begin() + i * s.dataStride);
                    s.time.erase(s.time.begin(), s.time.begin() + i);
                }

                if (s.time.size() != s.num) {
                    //LOG_D << __FUNCTION__ << LOG_EXPR(s.time.size()) << LOG_EXPR(s.num);
                    throw std::runtime_error("data time vector corrupt (length)");
                }

                if (s.num > 0 && s.t(0) != tr[0]) {
                    //LOG_D << __FUNCTION__ << LOG_EXPR(s.t(0)) << LOG_EXPR(tr[0])
                    //      << LOG_EXPR(tr[0] - s.t(0));
                    throw std::runtime_error(p.first + " data time vector corrupt");
                }

                sliced += i;
            } else if (s.t(0) > tr[0]) {
                auto ni = (s.t(0) - tr[0]) / sampling.intervalMs;
                s.num += ni;
                s.data.insert(s.data.begin(), s.dataStride * ni, 0);
                s.time.insert(s.time.begin(), ni, 0); // todo time compact
                for (size_t i = 0; i < ni; ++i)
                    s.time[i] = tr[0] + i * sampling.intervalMs;
                sliced += ni;
            }
        }
    }*/



}