#pragma once

#include "rapidjson/reader.h"
#include "client.h"

namespace {
    using namespace rapidjson;
    using namespace std;
    using  namespace influxdb;
struct ColumnReader {
    bool inColArray = false;
    std::vector<std::string> columns;

    bool Key(const char *str, SizeType length, bool copy) {
        if (strncmp(str, "columns", length) == 0) inColArray = true;
        return true;
    }

    bool String(const char *str, SizeType length, bool copy) {
        if (inColArray) columns.push_back(str);
        return true;
    }

    bool EndArray(SizeType elementCount) { return !inColArray; }


    bool Null() { return true; }

    bool Bool(bool b) { return true; }

    bool Int(int i) { return true; }

    bool Uint(unsigned u) { return true; }

    bool Int64(int64_t i) { return true; }

    bool Uint64(uint64_t u) { return true; }

    bool Double(double d) { return true; }

    bool RawNumber(const char *str, SizeType length, bool copy) { return true; }

    bool StartObject() { return true; }

    bool EndObject(SizeType memberCount) { return true; }

    bool StartArray() { return true; }
};


struct DataReader {
    const int numColumns;


    client::fetchResult &result;
    int inDataArray = 0;
    int colIndex = 0;

    uint64_t rowTime; //unused!


    DataReader(size_t numColumns, client::fetchResult &res) : numColumns((int) numColumns), result(res) {
    }


    bool Key(const char *str, SizeType length, bool copy) {
        if (strncmp(str, "values", length) == 0)
            ++inDataArray;
        return true;
    }

    bool StartArray() {
        if (inDataArray)
            ++inDataArray;
        return true;
    }

    bool Uint64(uint64_t u) {
        if (inDataArray == 3) {
            if (colIndex == 0) result.time.push_back(u);
            else  result.data.push_back(u);
            ++colIndex;
        }
        return true;
    }

    bool Double(double d) {
        if (inDataArray == 3) {
            if (colIndex > 0) result.data.push_back(d);
            else throw std::runtime_error("unexpected double");
            ++colIndex;
        }
        return true;
    }

    bool Uint(unsigned u) {
        if (inDataArray == 3) {
            if (colIndex > 0) result.data.push_back(u);
            else throw std::runtime_error("unexpected double");
            ++colIndex;
        }
        return true;
    }

    bool Null() {
        if (inDataArray == 3) {
            if (colIndex > 0) {
                result.data.push_back(
                        (result.data.size() < (numColumns - 1)) ? NAN : result.data[result.data.size() - (numColumns - 1)]); // repeat last
            } else throw std::runtime_error("unexpected null");
            ++colIndex;
        }
        return true;
    }

    bool String(const char *str, SizeType length, bool copy) {
        if (inDataArray) {
            throw std::runtime_error("unexpected string");
        }
        return true;
    }


    bool EndArray(SizeType elementCount) {
        if (inDataArray) {
            --inDataArray;
            if (inDataArray == 0) return false;
            else if (inDataArray == 2) {
                colIndex = 0;
            }
        }
        return true;
    }


    bool Bool(bool b) {
        if (inDataArray) {
            throw std::runtime_error("unexpected bool");
        }
        return true;
    }

    bool Int(int i) {
        if (inDataArray) {
            throw std::runtime_error("unexpected int");
        }
        return true;
    }


    bool Int64(int64_t i) {
        if (inDataArray) {
            throw std::runtime_error("unexpected int64");
        }
        return true;
    }


    bool RawNumber(const char *str, SizeType length, bool copy) {
        if (inDataArray) {
            throw std::runtime_error("unexpected raw number");
        }
        return true;
    }

    bool StartObject() {
        if (inDataArray) {
            throw std::runtime_error("unexpected object");
        }
        return true;
    }

    bool EndObject(SizeType memberCount) { return true; }


};
}