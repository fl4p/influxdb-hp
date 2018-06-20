#pragma once

#include <cmath>
#include "rapidjson/reader.h"
#include "client.h"

namespace influxdb {
    using namespace rapidjson;
    using namespace std;

    struct ColumnReader {
        bool inColArray = false;
        std::vector<std::string> columns;

        bool Key(const char *str, SizeType length, bool copy) {
            if (strncmp(str, "columns", length) == 0) inColArray = true;
            return true;
        }

        bool String(const char *str, SizeType length, bool copy) {
            if (inColArray) columns.emplace_back(str);
            return true;
        }

        bool EndArray(SizeType elementCount) { return !inColArray; /*stop after column array*/ }


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
                else result.data.push_back(u);
                ++colIndex;
            }
            return true;
        }

        bool Double(double d) {
            if (inDataArray == 3) {
                if (colIndex > 0) result.data.push_back(static_cast<float>(d));
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
                            (result.data.size() < (numColumns - 1)) ? NAN : result.data[result.data.size() -
                                                                                        (numColumns -
                                                                                         1)]); // repeat last
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
                throw std::runtime_error("DataReader: unexpected object");
            }
            return true;
        }

        bool EndObject(SizeType memberCount) { return true; }


    };


    struct SeriesReader {
        static constexpr int SeriesObjectLevel = 2;
        static constexpr int SeriesArrayLevelSeries = 2;
        static constexpr int SeriesArrayLevelRow = 3;
        static constexpr int SeriesArrayLevelCol = 4;

        const size_t numColumns;

        std::vector<series> &series_;
        series *currentSeries = nullptr;

        int inSeriesArray = 0;
        int lvObjects = -1;
        bool inTags = false;
        std::string currentTagKey;

        int colIndex = 0;

        uint64_t rowTime; //unused!


        SeriesReader(size_t numColumns, std::vector<series> &res) : numColumns(numColumns), series_(res) {}


        bool Key(const char *str, SizeType length, bool copy) {

            if (inTags) {
                currentTagKey = std::string(str, length);
            }

            if (inSeriesArray && !inTags && strncmp(str, "tags", length) == 0) {
                inTags = true;
            }

            if (strncmp(str, "series", length) == 0)
                ++inSeriesArray;

            return true;
        }

        bool StartArray() {
            if (inSeriesArray) {
                ++inSeriesArray;
            }
            return true;
        }

        bool EndArray(SizeType elementCount) {
            if (inSeriesArray) {
                --inSeriesArray;
                if (inSeriesArray == 0) return false;
                else if (inSeriesArray == 3) {
                    colIndex = 0;
                }
            }
            return true;
        }


        bool StartObject() {
            ++lvObjects;

            if (inSeriesArray == SeriesArrayLevelSeries && !currentSeries && lvObjects == SeriesObjectLevel) {
                series_.resize(series_.size() + 1);
                currentSeries = &series_.back();
            }

            if (inSeriesArray >= SeriesArrayLevelRow) {
                throw std::runtime_error("DataReader: unexpected object");
            }
            return true;
        }

        bool EndObject(SizeType memberCount) {
            if (currentSeries && lvObjects == SeriesObjectLevel) {
                currentSeries = nullptr;
            }

            if (inTags) {
                inTags = false;
            }

            --lvObjects;
            return true;
        }

        bool Uint64(uint64_t u) {
            if (inSeriesArray == SeriesArrayLevelCol) {
                if (colIndex == 0) currentSeries->time.push_back(u);
                else currentSeries->data.push_back(u);
                ++colIndex;
            }
            return true;
        }

        bool Double(double d) {
            if (inSeriesArray == SeriesArrayLevelCol) {
                if (!currentSeries) throw std::runtime_error("unexpected double (currentSeries null)");
                if (colIndex > 0) currentSeries->data.push_back(d);
                else throw std::runtime_error("unexpected double");
                ++colIndex;
            }
            return true;
        }

        bool Uint(unsigned u) {
            if (inSeriesArray == SeriesArrayLevelCol) {
                if (colIndex > 0) currentSeries->data.push_back(u);
                else throw std::runtime_error("unexpected double");
                ++colIndex;
            }
            return true;
        }

        bool Null() {
            if (inSeriesArray == SeriesArrayLevelCol) {
                if (colIndex > 0) {
                    auto &d{currentSeries->data};
                    d.push_back((d.size() < (numColumns - 1))
                                ? NAN
                                : d[currentSeries->data.size() - (numColumns - 1)]); // repeat previous
                } else throw std::runtime_error("unexpected null");
                ++colIndex;
            }
            return true;
        }

        bool String(const char *str, SizeType length, bool copy) {
            if (inSeriesArray >= SeriesArrayLevelCol) {
                throw std::runtime_error("SeriesReader: unexpected string " + std::string(str));
            }
            if (inTags) {
                //LOG_D << "tag " << currentTagKey << "=" << std::string(str);
                currentSeries->tags.emplace(std::move(currentTagKey), std::string{str, length});
            }
            return true;
        }


        bool Bool(bool b) {
            if (inSeriesArray >= SeriesArrayLevelRow) {
                throw std::runtime_error("unexpected bool");
            }
            return true;
        }

        bool Int(int i) {
            if (inSeriesArray >= SeriesArrayLevelRow) {
                throw std::runtime_error("unexpected int");
            }
            return true;
        }


        bool Int64(int64_t i) {
            if (inSeriesArray >= SeriesArrayLevelRow) {
                throw std::runtime_error("unexpected int64");
            }
            return true;
        }


        bool RawNumber(const char *str, SizeType length, bool copy) {
            if (inSeriesArray >= SeriesArrayLevelRow) {
                throw std::runtime_error("unexpected raw number");
            }
            return true;
        }


    };


}