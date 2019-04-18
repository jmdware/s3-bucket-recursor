/*
 * Copyright (C) 2019 David Ha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jmdware.s3br;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jmdware.s3br.Filters.Builder;
import org.junit.Test;

public class FiltersBuilderTest {

    private static final ZonedDateTime AFTER = ZonedDateTime.parse("2019-04-19T09:54:00-04:00[America/New_York]");

    private static final ZonedDateTime BEFORE = ZonedDateTime.parse("2019-04-20T04:01:00Z");

    private List<Predicate<String>> filters;

    private List<Boolean> actual;

    private String delimiter;

    @Test
    public void testBetweenArgChecking() {
        Builder builder = new Filters.Builder('/');

        Instant now = Instant.now();

        try {
            builder.between(now, now);
            fail("cannot be equals");
        } catch (RuntimeException e) { }

        try {
            builder.between(now.plus(1, ChronoUnit.NANOS), now);
            fail("min > max?!");
        } catch (RuntimeException e) { }
    }

    @Test
    public void testOnlyNonTemporal() {
        givenFiltersWithoutTemporal();

        thenFiltersSizeIs(3);

        whenEvaluated("foo/");

        thenNotAccepted();

        whenEvaluated("depth1/");

        thenAcceptedTo(0);

        whenEvaluated("depth1/foo/");

        thenAcceptedTo(0);

        whenEvaluated("depth1/depth1.1/");

        thenAcceptedTo(1);

        whenEvaluated("depth1/depth1.1/foo/");

        thenAcceptedTo(1);

        whenEvaluated("depth1/depth1.1/depth1.1.1/");

        thenAcceptedTo(2);

        whenEvaluated("depth1/depth1.1/depth1.1.2/");

        thenAcceptedTo(1);

        whenEvaluated("depth2/depth2.2/depth2.2.2/");

        thenAcceptedTo(2);
    }

    @Test
    public void testInsufficientTemporalGranularity() {
        Builder builder = new Builder('/')
                .after(AFTER)
                .addLevelTemporal("'m='m");

        try {
            builder.toFilters();
            fail("not enough temporal patterns to parse a point in time");
        } catch (RuntimeException e) { }

        builder.addLevelTemporal("'h='H");

        try {
            builder.toFilters();
            fail("not enough temporal patterns to parse a point in time");
        } catch (RuntimeException e) { }

        // if we have a year, then we can at least compare the prefix with the cutoff
        builder.addLevelTemporal("'y='yyyy")
                .toFilters();
    }

    @Test
    public void testHourThenDate() {
        givenFiltersWithHourThenDate();

        thenFiltersSizeIs(6);

        whenEvaluated("foo|");

        thenNotAccepted();

        whenEvaluated("depth0|");

        thenAcceptedTo(0);

        whenEvaluated("depth1|");

        thenAcceptedTo(0);

        whenEvaluated("depth1|h=8|");

        thenAcceptedTo(1);

        whenEvaluated("depth1|h=8|foo|");

        thenAcceptedTo(1);

        whenEvaluated("depth1|h=8|orders|");

        thenAcceptedTo(2);

        whenEvaluated("depth1|h=8|refunds|");

        thenAcceptedTo(2);

        whenEvaluated("depth1|h=8|refunds|dt=2019-04-19|");

        thenAcceptedTo(2);

        // try to trigger a date time parse exception with bad 'hour' string.
        whenEvaluated("depth1|hour=8|refunds|dt=2019-04-19|");

        thenAcceptedTo(2);

        whenEvaluated("depth1|h=9|refunds|dt=2019-04-19|");

        thenAcceptedTo(3);

        whenEvaluated("depth1|h=9|refunds|dt=2019-04-19|m=53|");

        thenAcceptedTo(3);

        whenEvaluated("depth1|h=9|refunds|dt=2019-04-19|m=54|");

        thenAcceptedTo(4);

        whenEvaluated("depth1|h=9|refunds|dt=2019-04-19|m=54|foo|");

        thenAcceptedTo(4);

        whenEvaluated("depth1|h=9|refunds|dt=2019-04-19|m=54|txt|");

        thenAcceptedTo(5);

        whenEvaluated("depth1|h=9|refunds|dt=2019-04-19|m=55|txt|");

        thenAcceptedTo(5);

        whenEvaluated("depth1|h=10|refunds|dt=2019-04-19|m=00|txt|");

        thenAcceptedTo(5);

        whenEvaluated("depth1|h=10|refunds|dt=2019-04-19|m=54|txt|");

        thenAcceptedTo(5);

        whenEvaluated("depth1|h=0|refunds|dt=2019-04-20||");

        thenAcceptedTo(3);

        whenEvaluated("depth1|h=0|refunds|dt=2019-04-20|m=00|");

        thenAcceptedTo(4);

        whenEvaluated("depth1|h=0|refunds|dt=2019-04-20|m=00|txt|");

        thenAcceptedTo(5);

        // when the allowed time ends
        whenEvaluated("depth1|h=0|refunds|dt=2019-04-20|m=01|txt|");

        thenAcceptedTo(3);
    }

    @Test
    public void testDateThenHour() {
        givenFiltersDateThenHour();

        thenFiltersSizeIs(8);

        whenEvaluated("foo/");

        thenNotAccepted();

        whenEvaluated("depth1/");

        thenAcceptedTo(0);

        whenEvaluated("depth1/dt=2019-04-18/");

        thenAcceptedTo(0);

        whenEvaluated("depth1/dt=2019-04-19/");

        thenAcceptedTo(1);

        whenEvaluated("depth1/dt=2019-04-19/foo/");

        thenAcceptedTo(1);

        whenEvaluated("depth1/dt=2019-04-19/orders/");

        thenAcceptedTo(2);

        whenEvaluated("depth1/dt=2019-04-19/refunds/");

        thenAcceptedTo(2);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=8/");

        thenAcceptedTo(2);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=9/");

        thenAcceptedTo(3);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=9/m=53/");

        thenAcceptedTo(3);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=9/m=54/");

        thenAcceptedTo(4);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=9/m=54/foo/");

        thenAcceptedTo(4);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=9/m=54/txt/");

        thenAcceptedTo(5);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=9/m=55/txt/");

        thenAcceptedTo(5);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=10/m=00/txt/");

        thenAcceptedTo(5);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=10/m=54/txt/");

        thenAcceptedTo(5);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=10/m=54/txt/src=us/");

        thenAcceptedTo(6);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=10/m=54/txt/src=us/dst=us/");

        thenAcceptedTo(6);

        whenEvaluated("depth1/dt=2019-04-19/refunds/h=10/m=54/txt/src=us/dst=eu/");

        thenAcceptedTo(7);

        whenEvaluated("depth1/dt=2019-04-20/");

        thenAcceptedTo(1);

        whenEvaluated("depth1/dt=2019-04-20/refunds/");

        thenAcceptedTo(2);

        whenEvaluated("depth1/dt=2019-04-20/refunds/h=0/");

        thenAcceptedTo(3);

        whenEvaluated("depth1/dt=2019-04-20/refunds/h=0/m=00/");

        thenAcceptedTo(4);

        whenEvaluated("depth1/dt=2019-04-20/refunds/h=0/m=00/txt/");

        thenAcceptedTo(5);
    }

    @Test
    public void testMinuteBeforeHour() {
        givenFiltersWithMinuteBeforeHour();

        thenFiltersSizeIs(4);

        whenEvaluated("m=53/");

        thenAcceptedTo(0);

        whenEvaluated("m=53/dt=20190418/");

        thenAcceptedTo(0);

        whenEvaluated("m=53/dt=20190419/");

        thenAcceptedTo(1);

        whenEvaluated("m=53/dt=20190419/foo/");

        thenAcceptedTo(2);

        whenEvaluated("m=53/dt=20190419/foo/h=8/");

        thenAcceptedTo(2);

        whenEvaluated("m=53/dt=20190419/foo/h=9/");

        thenAcceptedTo(2);

        whenEvaluated("m=54/");

        thenAcceptedTo(0);

        whenEvaluated("m=54/dt=20190418/");

        thenAcceptedTo(0);

        whenEvaluated("m=54/dt=20190419/");

        thenAcceptedTo(1);

        whenEvaluated("m=54/dt=20190419/foo/");

        thenAcceptedTo(2);

        whenEvaluated("m=54/dt=20190419/foo/h=8/");

        thenAcceptedTo(2);

        whenEvaluated("m=54/dt=20190419/foo/h=9/");

        thenAcceptedTo(3);

        whenEvaluated("m=55/");

        thenAcceptedTo(0);

        whenEvaluated("m=55/dt=20190418/");

        thenAcceptedTo(0);

        whenEvaluated("m=55/dt=20190419/");

        thenAcceptedTo(1);

        whenEvaluated("m=55/dt=20190419/foo/");

        thenAcceptedTo(2);

        whenEvaluated("m=55/dt=20190419/foo/h=8/");

        thenAcceptedTo(2);

        whenEvaluated("m=55/dt=20190419/foo/h=9/");

        thenAcceptedTo(3);
    }

    private void givenFiltersWithoutTemporal() {
        delimiter = Character.toString('/');
        filters = new Builder('/')
                .addLevelRegex("depth\\d+")
                .addLevelRegex("depth\\d+\\.\\d+")
                .addRelativeRegex("depth(\\d+)/depth\\1\\.\\1/depth\\1\\.\\1\\.\\1/")
                .toFilters();
    }

    private void givenFiltersWithHourThenDate() {
        delimiter = Character.toString('|');
        filters = new Builder('|')
                .between(AFTER.toString(), BEFORE.toString())
                .addLevelRegex("depth\\d+")
                .addLevelTemporal("'h='H")
                .addLevelRegex("(orders|refunds)")
                .addLevelTemporal("'dt='yyyy'-'MM'-'dd")
                .addLevelTemporal("'m='mm")
                .addLevelRegex("txt")
                .toFilters();
    }

    private void givenFiltersDateThenHour() {
        delimiter = Character.toString('/');
        filters = new Builder('/')
                .after(AFTER.toString())
                .addLevelRegex("depth1")
                .addLevelTemporal("'dt='yyyy'-'MM'-'dd")
                .addLevelRegex("(orders|refunds)")
                .addLevelTemporal("'h='H")
                .addLevelTemporal("'m='mm")
                .addLevelRegex("txt")
                .addLevelAnything()
                .addRelativeRegex(".*/src=(\\w+)/dst=(?!\\1).+/")
                .toFilters();
    }

    private void givenFiltersWithMinuteBeforeHour() {
        delimiter = Character.toString('/');
        filters = new Builder('/')
                .after(AFTER.toInstant(), AFTER.getZone())
                .addLevelTemporal("'m='m")
                .addLevelTemporal("'dt='yyyyMMdd")
                .addLevelRegex("foo")
                .addLevelTemporal("'h='H")
                .toFilters();
    }

    private void whenEvaluated(String prefix) {
        // s3 api always prefixes that end with the delimiter
        assertThat("broken test setup", prefix, not(startsWith(delimiter)));
        assertThat("broken test setup", prefix, endsWith(delimiter));

        actual = new ArrayList<>(filters.size());

        for (Predicate<String> filter : filters) {
            actual.add(filter.test(prefix));
        }
    }

    private void thenNotAccepted() {
        assertThat(actual, not(hasItem(true)));
    }

    private void thenAcceptedTo(int idx) {
        List<Boolean> expected = IntStream.range(0, filters.size())
                .mapToObj((i) -> i <= idx)
                .collect(Collectors.toList());

        assertThat(actual, equalTo(expected));
    }

    private void thenFiltersSizeIs(int expected) {
        assertThat(filters.size(), equalTo(expected));
    }
}
