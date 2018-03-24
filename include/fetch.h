#include <string>
#include <vector>

#include "../../pclog/pclog.h"
#include "../../pclog/to_string.h"


namespace influxdb {
    struct fetchResult {
        size_t num = 0;
        size_t dataStride = 0;
        std::vector<std::string> columns;
        std::vector<float> data;
        std::vector<uint64_t> time;
    };

    template<typename T>
    static void _write(std::ostream &s, const T &v) {
        s.write(reinterpret_cast<const char *>(&v), sizeof(T));
    }


    template<typename T>
    static void _read(std::istream &s, T &v) {
        s.read(reinterpret_cast<char *>(&v), sizeof(T));
    }

    static std::ostream &operator<<(std::ostream &s, const fetchResult &fr) {
        auto cn = fr.columns.size();
        _write(s, cn), _write(s, fr.num), _write(s, fr.dataStride);
        _write(s,'\n');
        for (auto &col:fr.columns) s << col << " ";
        _write(s,'\n');
        for (size_t i = 0; i < fr.num; ++i) {
            _write(s, fr.time[i]);
            for (size_t ci = 0; ci < fr.dataStride; ++ci) _write(s, fr.data[i * fr.dataStride + ci]);
        }
        return s;
    }

    static std::istream &operator>>(std::istream &s, fetchResult &fr) {
        size_t cn;
        _read(s, cn), _read(s, fr.num), _read(s, fr.dataStride);
        fr.columns.resize(cn);
        fr.time.resize(fr.num);
        fr.data.resize(fr.num * fr.dataStride);
       // LOG_E << LOG_EXPR(cn)<< LOG_EXPR(fr.num) << LOG_EXPR(fr.dataStride);
        if(s.get() != '\n') throw std::runtime_error("invalid fetchResult header (1)");
        for (int i = 0; i < cn; ++i) s >> fr.columns[i];
       // LOG_E << fr.columns[0];
        if( s.get() != '\n' && s.get() != '\n') throw std::runtime_error("invalid fetchResult header (2) ");

        for (size_t i = 0; i < fr.num; ++i) {
            _read(s, fr.time[i]);
            for (size_t ci = 0; ci < fr.dataStride; ++ci) _read(s, fr.data[i * fr.dataStride + ci]);
        }
        if(s.fail()) throw std::runtime_error("stream fail after fetchResult read");
        return s;
    }
}