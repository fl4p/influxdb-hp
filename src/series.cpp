#include <algorithm>
#include <cmath>
#include "series.h"

namespace influxdb {
    void series::joinInner(const series &other) {
        size_t selfA = 0, otherA = 0;
        while (other.t(otherA) < t(0)) ++otherA;
        while (other.t(otherA) != t(selfA)) {
            ++selfA;
            if (selfA >= num) throw std::runtime_error("cannot join series with different sampling interval");
        }


        std::copy(other.columns.begin() + 1, other.columns.end(), std::back_inserter(columns));

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
        dataStride += other.dataStride;
        data = std::move(joint);

        // TODO time compacted!
        time.erase(time.begin(), time.begin() + selfA);
        time.erase(time.begin() + num, time.end());
    }


    size_t series::fill() {

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

        // fill time sampling gaps with previous
        auto si = t(1) - t(0);
        auto lastT = t(0);
        for (size_t i = 1; i < num; ++i) {
            auto nIns = (t(i) - lastT) / si - 1;
            if (nIns) {
                // insert repeating previous
                time.insert(time.begin() + i, nIns, 0);
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
        size_t i = 0;
        for (i = 0; i < num; ++i) {
            bool hasNan = false;
            for (size_t c = 0; c < dataStride; ++c) {
                if (std::isnan(data[i * dataStride + c])) {
                    hasNan = true;
                    break;
                }
            }
            if (!hasNan) break;
        }

        if (i > 0) {
            num -= i;
            time.erase(time.begin(), time.begin() + i);
            data.erase(data.begin(), data.begin() + i * dataStride);
        }

        return i;
    }

}