#pragma  once

#include <iostream>
#include <sstream>
#include <iomanip>
#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <windows.h>
#include <ws2tcpip.h>
#endif

//#include <date/date.h>
#include "../date/include/date/date.h"

namespace influxdb {
	namespace util {
		using namespace date;
		using namespace std;

		static std::string formatByte(unsigned char value) {
			std::stringstream ss;
			ss << std::setfill('0') << std::setw(2) << std::hex << std::uppercase
			   << static_cast<unsigned int>(value);
			return ss.str();
		}

		static std::string urlEncode(const std::string &stri) {
			std::string result;
			result.reserve(stri.length() + 1);
			auto str = stri.c_str();
			for (char ch = *str; ch != '\0'; ch = *++str) {
				switch (ch) {
					case '%':
					case '=':
					case '&':
					case '\n':
					case ' ':
						result.append("%" + formatByte(static_cast<unsigned char>(ch)));
						break;
					default:
						result.push_back(ch);
						break;
				}
			}
			return result;
		}

/*
        std::string timeToStr(long epoch) {
            time_t now = epoch;
            char buf[sizeof "2011-10-08T07:07:09Z"];
            strftime(buf, sizeof buf, "%FT%TZ", gmtime(&now));
            return std::string(buf);
        }


        long strToTime(const std::string &str) {
            const char *dateStr = "2014-11-12T19:12:14.505Z";
            int y,M,d,h,m;
            float s;
            sscanf(dateStr, "%d-%d-%dT%d:%d:%fZ", &y, &M, &d, &h, &m, &s);

            tm time;
            time.tm_year = y - 1900; // Year since 1900
            time.tm_mon = M - 1;     // 0-11
            time.tm_mday = d;        // 1-31
            time.tm_hour = h;        // 0-23
            time.tm_min = m;         // 0-59
            time.tm_sec = (int)s;    // 0-61 (0-60 in C++11)

            return mktime(&tm);
        }
        */


		static date::sys_time<std::chrono::milliseconds>
		parse8601(std::istream &&is) {
			std::string save;
			is >> save;

			try {

				std::istringstream in{save};
				date::sys_time<std::chrono::milliseconds> tp;
				in >> date::parse("%FT%TZ", tp);
				if (in.fail()) {
					in.clear();
					in.exceptions(std::ios::failbit);
					in.str(save);
					in >> date::parse("%FT%T%Ez", tp);
				}
				return tp;
			} catch (const std::exception &e) {
				throw std::runtime_error("parse8601('" + save + "') failed");
			}
		}

		static date::sys_time<std::chrono::milliseconds>
		parseHttpDate(std::istream &&is) {
			std::string save;
			is >> save;
			try {
				static std::locale enUS("en_US");
				std::istringstream in{save};
				in.imbue(enUS);
				date::sys_time<std::chrono::milliseconds> tp;
				// Tue, 15 Nov 1994 12:45:26 GMT
				in >> date::parse("%a, %d %b %Y %T %Z", tp); // "D, d M Y H:i:s T%FT%TZ"
				return tp;
			} catch (const std::exception &e) {
				throw std::runtime_error("parseHttpDate('" + save + "') failed");
			}
		}

		inline date::sys_time<std::chrono::milliseconds>
		parse8601(const std::string &str) { return parse8601(istringstream{str}); }

		inline date::sys_time<std::chrono::milliseconds>
		parseHttpDate(const std::string &str) { return parseHttpDate(istringstream{str}); }

		//   inline std::string to8601(const date::sys_time<std::chrono::milliseconds> &tp) {
		//      return date::format("%FT%TZ", tp);
		//   }

		inline std::string to8601(int64_t epochMs) {
			const static date::sys_time<std::chrono::milliseconds> tp_epoch;
			return date::format("%FT%TZ", tp_epoch + std::chrono::milliseconds(epochMs));
		}

		// inline std::string to8601(const std::chrono::time_point<std::chrono::milliseconds> &tp) {
		//      return to8601(tp.time_since_epoch().count());
		// }


		//inline std::string to8601(const std::chrono::system_clock::time_point &tp) {
		//	return to8601(std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count());
		//}

		template<class T, typename = std::enable_if_t<!std::is_integral<T>::value>>
		inline std::string to8601(const T &tp) {
			return to8601(std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count());
		}

		template<class T, typename = std::enable_if_t<!std::is_integral<T>::value>>
		inline std::string to8601(const T &&tp) {
			return to8601(std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count());
		}

		//template<>
		//inline std::string to8601<int64_t>(const int64_t &tp) { return to8601_(tp); }


		static bool replace(std::string &str, const std::string &from, const std::string &to) {
			size_t start_pos = str.find(from);
			if (start_pos == std::string::npos)
				return false;
			str.replace(start_pos, from.length(), to);
			return true;
		}
	}

	inline int wsaStart() {
#if defined(_WIN32) && defined(MAKEWORD)
		WORD wVersionRequested;
		WSADATA wsaData;
		wVersionRequested = MAKEWORD(2, 2);
		WSAStartup(wVersionRequested, &wsaData);
#endif
		return 0;
	}


}
