#ifdef MINGW
# define __LITTLE_ENDIAN__
#endif

#include "../farmhash/src/farmhash.h"
#include "../cpp-base64/base64.h"
#include <string>
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>


namespace influxdb {


    template<typename T>
    class file_cache {

        static int mkdir(std::string dir) {
#ifdef WIN32
            return ::mkdir(dir.c_str());
#else
            return ::mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
#endif
        }

        static std::string fingerprint128_base64(const std::string &key) {
            auto fp = ::util::Fingerprint128(key);
            auto b1 = base64_encode(reinterpret_cast<const unsigned char *>(&fp.first), 8);
            auto b2 = base64_encode(reinterpret_cast<const unsigned char *>(&fp.second), 8);
            auto b = b1.substr(0, b1.length() - 1) + b2.substr(0, b2.length() - 1);
            std::replace(b.begin(), b.end(), '+', '-');
            std::replace(b.begin(), b.end(), '/', '_');
            return b;
        }

        std::pair<std::string, std::string> dirAndFile(const std::string &key) const {
            auto b64 = fingerprint128_base64(key);
            auto d = dir + '/' + b64.substr(0, 2);
            return {d, d + '/' + b64.substr(2)};
        }

    public:
        const std::string dir;

        explicit file_cache(std::string dir) : dir{dir} {
            mkdir(dir.c_str());
        }

        bool get(std::string key, T &v) const {
            auto df = dirAndFile(key);
            std::ifstream f(df.second);
            if (!f.good()) return false;
            f >> v;
            return true;
        }

        bool have(std::string key) const {
            auto df = dirAndFile(key);
            std::ifstream f(df.second);
           // LOG_W << "have:" << LOG_EXPR(key) << LOG_EXPR(df.second) << LOG_EXPR(f.good());
            return f.good();
        }

        std::future<bool> get_async(std::string key, T &v) const {
            auto df = dirAndFile(key);
            return std::move(std::async(std::launch::async, [df, &v]() {
                std::ifstream f(df.second);
                if (!f.good()) return false;
                f >> v;
                return true;
            }));
        }

        std::future<void> get_async_throw(std::string key, T &v) const {
            auto df = dirAndFile(key);
            return std::move(std::async(std::launch::async, [df, &v]() {
                std::ifstream f(df.second);
                //LOG_W << "reading" << df.second;
                if (!f.good()) throw std::runtime_error("not found in file cache");
                f >> v;
            }));
        }

        void set(const std::string & key, const T &v) const {
            //LOG_D << "set:" << LOG_EXPR(key);
            auto df = dirAndFile(key);
          //  std::cout << df.second << std::endl;
            std::ofstream f(df.second);
            if (!f.good()) {
                mkdir(df.first.c_str());
                f.open(df.second);
                if (!f.good()) throw std::runtime_error("cant open " + df.second + " for writing");
            }
            //LOG_W << "\nwriting " << df.second << "\nkey=" << key;
            f << v;
            if (!f.good()) throw std::runtime_error("write failure with " + df.second);
        }

    };
}