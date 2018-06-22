#include <cmath>
#include <gtest/gtest.h>


#include "../include/client.h"
#include "../src/util.h"


/*

curl -i -XPOST 'http://localhost:8086/write?db=test' --data-binary '
load,host=s01,region=us v=0.23 1529425348000000000
load,host=s01,region=us v=0.26 1529425349000000000
load,host=s01,region=us v=0.21 1529425350000000000
load,host=s01,region=us v=0.18 1529425353000000000'

> select last(v) from load group by time(1s) fill(previous) limit 10
2018-06-19T16:22:28Z    0.23
2018-06-19T16:22:29Z    0.26
2018-06-19T16:22:30Z    0.21
2018-06-19T16:22:31Z    0.21
2018-06-19T16:22:32Z    0.21
2018-06-19T16:22:33Z    0.18
2018-06-19T16:22:34Z    0.18
2018-06-19T16:22:35Z    0.18
2018-06-19T16:22:36Z    0.18
2018-06-19T16:22:37Z    0.18

 */


TEST(InfluxDB, fetch) {
    using namespace influxdb;
    using namespace influxdb::util;
    using namespace std::chrono_literals;

    client c("localhost", 8086, "test");
    auto s0 = c.fetch("SELECT last(v) as v FROM load WHERE :time_condition: GROUP BY time(1s) FILL(previous) LIMIT 10",
                      {"2018-06-19T16:22:26Z", "2018-06-19T16:22:40Z"});

    ASSERT_EQ(s0.columns.size(), 2);
    ASSERT_EQ(s0.columns[0], "time");
    ASSERT_EQ(s0.columns[1], "v");

    ASSERT_EQ(s0.num, 10);
    ASSERT_EQ(s0.dataStride, 1);

    ASSERT_EQ(influxdb::util::to8601(s0.t(0)), "2018-06-19T16:22:26.000Z");
    ASSERT_EQ(influxdb::util::to8601(s0.t(1)), "2018-06-19T16:22:27.000Z");

    LOG_D << s0.data;
    ASSERT_TRUE(std::isnan(s0.data[0]));
    ASSERT_TRUE(std::isnan(s0.data[1]));
    ASSERT_NEAR(s0.data[2], 0.23, 1e-7);

    std::vector<double> ref0{0.23, 0.26, 0.21, 0.21, 0.21, 0.18, 0.18, 0.18};
    for (size_t i = 0; i < ref0.size(); ++i) {
        ASSERT_NEAR(s0.data[2 + i], ref0[i], 1e-7);
    }
}


TEST(InfluxDB, join) {
    using namespace influxdb;
    using namespace influxdb::util;
    using namespace std::chrono_literals;

    client c("localhost", 8086, "test");
    auto s0 = c.fetch("SELECT last(v) as v FROM load WHERE :time_condition: GROUP BY time(1s) FILL(previous) LIMIT 10",
                      {"2018-06-19T16:22:26Z", "2018-06-19T16:22:40Z"}
    );

    auto s1 = c.fetch(
            "SELECT last(v)*2 as v2, count(v) as n FROM load WHERE :time_condition: GROUP BY time(1s) FILL(previous) LIMIT 10",
            {"2018-06-19T16:22:25Z"/* +1sec */, "2018-06-19T16:22:41Z"/* +1sec */}
    );

    auto s0Or = s0;
    s0.joinInner(s1);

    ASSERT_EQ(s0.dataStride, s0Or.dataStride + s1.dataStride);

    ASSERT_EQ(s0.num, s0Or.num - 1);
    ASSERT_EQ(s0.num, s1.num - 1);
    ASSERT_EQ(s0.num, s0.tSize());
    ASSERT_EQ(s0.columns[0], "time");
    ASSERT_EQ(s0.columns[1], "v");
    ASSERT_EQ(s0.columns[2], "v2");
    ASSERT_EQ(s0.columns[3], "n");

    for (int i = 0; i < s0.num; ++i) {
        EXPECT_EQ(s0.t(i), s1.t(i + 1));
    }

    for (int i = 2; i < s0.num; ++i) {
        EXPECT_NEAR(s0.data[i * s0.dataStride] * 2, s0.data[i * s0.dataStride + 1], 1e-7);
    }

    LOG_D << s0.data;
}


TEST(InfluxDBSeries, fill) {
    using namespace influxdb;

    int64_t si = 100;

    series s0;
    // construct sample data
    s0.data = {
            2.0f, 2.1f, 700.f,
            2.2f, 2.3f, 800.f,
            // 1 sample gap
            3.0f, 3.4f, 1000.0f
    };
    s0.getTimeVector() = {
            1000l,
            1000l + si,
            // 1 sample gap
            1000l + 3l * si
    };
    s0.num = 3;
    s0.dataStride = 3;

    s0.fill();

    ASSERT_EQ(s0.num, 4);
    ASSERT_EQ(s0.num, s0.tSize());
    ASSERT_EQ(s0.t(2), 1000ul + 2ul * si);

    ASSERT_EQ(s0.data[2 * 3 + 0], s0.data[1 * 3 + 0]);
    ASSERT_EQ(s0.data[2 * 3 + 1], s0.data[1 * 3 + 1]);
    ASSERT_EQ(s0.data[2 * 3 + 2], s0.data[1 * 3 + 2]);

    s0.fill();
    ASSERT_EQ(s0.num, 4);
}



TEST(InfluxDBSeries, fill2) {
    using namespace influxdb;

    int64_t si = 100;

    series s0;
    // construct sample data
    s0.data = {
            2.0f, 2.1f, 700.f,
            2.2f, 2.3f, 800.f,
            // 2 sample gap
            3.0f, 3.4f, 1000.0f
    };
    s0.getTimeVector() = {
            1000l,
            1000l + si,
            // 2 sample gap
            1000l + 4l * si
    };
    s0.num = 3;
    s0.dataStride = 3;

    s0.fill();

    ASSERT_EQ(s0.num, 5);
    ASSERT_EQ(s0.num, s0.tSize());
    ASSERT_EQ(s0.t(2), 1000ul + 2ul * si);
    ASSERT_EQ(s0.t(3), 1000ul + 3ul * si);

    ASSERT_EQ(s0.data[2 * 3 + 0], s0.data[1 * 3 + 0]);
    ASSERT_EQ(s0.data[2 * 3 + 1], s0.data[1 * 3 + 1]);
    ASSERT_EQ(s0.data[2 * 3 + 2], s0.data[1 * 3 + 2]);

    s0.fill();
    ASSERT_EQ(s0.num, 5);
}


TEST(InfluxDB, trim) {
    using namespace influxdb;
    using namespace influxdb::util;
    using namespace std::chrono_literals;

    client c("localhost", 8086, "test");
    auto s0 = c.fetch("SELECT last(v) as v FROM load WHERE :time_condition: GROUP BY time(1s) FILL(previous) LIMIT 10",
                      {"2018-06-19T16:22:26Z", "2018-06-19T16:22:40Z"});

    ASSERT_EQ(s0.num, 10);

    ASSERT_TRUE(std::isnan(s0.data[0]));
    ASSERT_TRUE(std::isnan(s0.data[1]));

    s0.trim();

    ASSERT_EQ(s0.num, 8);
    ASSERT_NEAR(s0.data[0], 0.23, 1e-7);
}