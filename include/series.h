#include <string>
#include <vector>
#include <unordered_map>
#include <cmath>

#include "../../pclog/pclog.h"
#include "../../pclog/to_string.h"


namespace influxdb {
    struct series;
    struct SeriesReader;
    struct DataReader;

    static std::istream &operator>>(std::istream &s, series &fr);

    struct series {
        friend struct SeriesReader;
        friend struct DataReader;

        friend std::istream &operator>>(std::istream &s, series &fr);

        std::string name{};
        std::unordered_map<std::string, std::string> tags{};
        std::vector<std::string> columns{};
        size_t num{0};
        size_t dataStride{0};
        std::vector<float> data{};
    public:
        std::vector<int64_t> time{};

    public:

        inline void clear() {
            data.clear();
            time.clear();
            num = 0;
        }

        inline int64_t t(size_t frame) const { return time[frame]; }

        inline int64_t tEnd() const { return t(num - 1); }

        inline size_t tSize() const { return time.size(); }

        inline bool tIsCompact() const { return time.size() == 2; }

        void joinInner(const series &other);

        size_t fill();

        size_t trim();

        template<class F>
        size_t trim(const F &pred) {
            size_t i = 0;
            for (i = 0; i < num; ++i) {
                if (pred(data.data() + i * dataStride, dataStride))
                    break;
            }

            if (i > 0) {
                num -= i;
                time.erase(time.begin(), time.begin() + i);
                data.erase(data.begin(), data.begin() + i * dataStride);
            }

            return i;
        }

        static series sortedMerge(std::vector<series> &results);


        void checkNum() {
            if (num != time.size()) {
                throw std::runtime_error("unexpected time len");
            }

            if (dataStride != (columns.size() - 1)) {
                throw std::runtime_error("unexpected columns size");
            }

            if (num != data.size() / dataStride) {
                throw std::runtime_error("unexpected data size");
            }
        }
    };


    typedef series fetchResult;

    template<typename T>
    static void _write(std::ostream &s, const T &v) {
        s.write(reinterpret_cast<const char *>(&v), sizeof(T));
    }


    template<typename T>
    static void _readVal(std::istream &s, T &v) {
        s.read(reinterpret_cast<char *>(&v), sizeof(T));
    }

    static std::ostream &operator<<(std::ostream &s, const fetchResult &fr) {
        auto cn = fr.columns.size();
        _write(s, cn), _write(s, fr.num), _write(s, fr.dataStride);
        _write(s, '\n');
        for (auto &col:fr.columns) s << col << " ";
        _write(s, '\n');
        for (size_t i = 0; i < fr.num; ++i) {
            _write(s, fr.t(i));
            for (size_t ci = 0; ci < fr.dataStride; ++ci) _write(s, fr.data[i * fr.dataStride + ci]);
        }
        return s;
    }

    static std::istream &operator>>(std::istream &s, fetchResult &fr) {
        size_t cn;
        _readVal<size_t>(s, cn), _readVal(s, fr.num), _readVal<size_t>(s, fr.dataStride);
        fr.columns.resize(cn);
        fr.time.resize(fr.num);
        fr.data.resize(fr.num * fr.dataStride);
        // LOG_E << LOG_EXPR(cn)<< LOG_EXPR(fr.num) << LOG_EXPR(fr.dataStride);
        if (s.get() != '\n') throw std::runtime_error("invalid fetchResult header (1)");
        for (int i = 0; i < cn; ++i) s >> fr.columns[i];
        // LOG_E << fr.columns[0];
        if (s.get() != '\n' && s.get() != '\n') throw std::runtime_error("invalid fetchResult header (2) ");

        for (size_t i = 0; i < fr.num; ++i) {
            _readVal(s, fr.time[i]);
            for (size_t ci = 0; ci < fr.dataStride; ++ci) _readVal(s, fr.data[i * fr.dataStride + ci]);
        }
        if (s.fail()) throw std::runtime_error("stream fail after fetchResult read");
        return s;
    }
}