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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jmdware.s3br.Filters.Builder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import io.findify.s3mock.S3Mock;

public class BucketRecursorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketRecursorTest.class);

    private S3Mock s3;

    private AmazonS3 client;

    private List<String> actual;

    @Before
    public void before() {
        int port = 0;

        while (port == 0) {
            try {
                int portAttempt = ThreadLocalRandom.current().nextInt(65535 - 1024) + 1024;

                S3Mock s3Attempt = new S3Mock.Builder()
                        .withPort(portAttempt)
                        .build();
                s3Attempt.start();

                port = portAttempt;
                s3 = s3Attempt;
            } catch (RuntimeException e) {
                // sometimes the previous test doesn't release the port before the next test claims
                // one so use a random port each time.
                LOGGER.info("trapped exception", e);
            }
        }

        client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new EndpointConfiguration(
                        "http://localhost:" + port,
                        "us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();

        client.createBucket("bucket");
    }

    @After
    public void after() {
        s3.stop();
    }

    @Test
    public void testDepth() {
        givenObject("root/prefix/date=20190414/2019-04-14T03.1.txt");
        givenObject("root/prefix/date=20190414/2019-04-14T03.2.txt");
        givenObject("root/prefix/date=20190414/2019-04-14T03.3.txt");

        givenObject("root/prefix/date=20190415/2019-04-15T03.1.txt");
        givenObject("root/prefix/date=20190415/2019-04-15T03.2.txt");
        givenObject("root/prefix/date=20190415/2019-04-15T03.3.txt");

        givenObject("root/prefix/date=20190416/2019-04-16T03.1.txt");
        givenObject("root/prefix/date=20190416/2019-04-16T03.2.txt");
        givenObject("root/prefix/date=20190416/2019-04-16T03.3.txt");

        givenObject("root/prefix/date=20190417/2019-04-17T03.1.txt");
        givenObject("root/prefix/date=20190417/2019-04-17T03.2.txt");
        givenObject("root/prefix/date=20190417/2019-04-17T03.3.txt");

        whenStreamedForDepth();

        thenReturned(
                "root/prefix/date=20190414/2019-04-14T03.1.txt",
                "root/prefix/date=20190414/2019-04-14T03.2.txt",
                "root/prefix/date=20190414/2019-04-14T03.3.txt",

                "root/prefix/date=20190415/2019-04-15T03.1.txt",
                "root/prefix/date=20190415/2019-04-15T03.2.txt",
                "root/prefix/date=20190415/2019-04-15T03.3.txt",

                "root/prefix/date=20190416/2019-04-16T03.1.txt",
                "root/prefix/date=20190416/2019-04-16T03.2.txt",
                "root/prefix/date=20190416/2019-04-16T03.3.txt",

                "root/prefix/date=20190417/2019-04-17T03.1.txt",
                "root/prefix/date=20190417/2019-04-17T03.2.txt",
                "root/prefix/date=20190417/2019-04-17T03.3.txt");
    }

    @Test
    public void testFilteringDateThenHour() {
        givenObject("root/prefix/date=20181231/hour=9/file1.txt");
        givenObject("root/prefix/date=20181231/hour=9/file1.txt");
        givenObject("root/prefix/date=20181231/hour=9/file2.txt");
        givenObject("root/prefix/date=20181231/hour=9/file3.txt");

        givenObject("root/prefix/date=20181231/hour=15/file1.txt");
        givenObject("root/prefix/date=20181231/hour=15/file2.txt");
        givenObject("root/prefix/date=20181231/hour=15/file3.txt");

        givenObject("root/prefix/date=20190101/hour=16/file1.txt");
        givenObject("root/prefix/date=20190101/hour=16/file2.txt");
        givenObject("root/prefix/date=20190101/hour=16/file3.txt");

        givenObject("root/prefix/date=20190102/hour=17/file1.txt");
        givenObject("root/prefix/date=20190102/hour=17/file2.txt");
        givenObject("root/prefix/date=20190102/hour=17/file3.txt");

        givenObject("root/prefix/date=20190102/extraneous-file.txt");
        givenObject("root/prefix/not-a-date/hour=9/file1.txt");
        givenObject("root/prefix/date=20190102/not-hour=17/file1.txt");
        givenObject("root/prefix/date=20190102/hour=17/extraneous-prefix/file3.txt");

        whenStreamedDateThenHourFilters();

        thenReturned(
                "root/prefix/date=20181231/hour=15/file1.txt",
                "root/prefix/date=20181231/hour=15/file2.txt",
                "root/prefix/date=20181231/hour=15/file3.txt",

                "root/prefix/date=20190101/hour=16/file1.txt",
                "root/prefix/date=20190101/hour=16/file2.txt",
                "root/prefix/date=20190101/hour=16/file3.txt",

                "root/prefix/date=20190102/hour=17/file1.txt",
                "root/prefix/date=20190102/hour=17/file2.txt",
                "root/prefix/date=20190102/hour=17/file3.txt");
    }

    @Test
    public void testFilteringDateWithHourThenMinute() {
        givenObject("root/prefix/20181231T09/5/purchases/file.txt");
        givenObject("root/prefix/20181231T09/6/purchases/file.txt");
        givenObject("root/prefix/20181231T09/7/refunds/file.txt");

        givenObject("root/prefix/20181231T15/10/purchases/file.txt");
        givenObject("root/prefix/20181231T15/11/refunds/file.txt");
        givenObject("root/prefix/20181231T15/12/surveys/file.txt");

        givenObject("root/prefix/20190101T16/50/purchases/file.txt");
        givenObject("root/prefix/20190101T16/51/purchases/file1.txt");
        givenObject("root/prefix/20190101T16/51/refunds/file2.txt");

        givenObject("root/prefix/20190102T17/27/surveys/file1.txt");
        givenObject("root/prefix/20190102T17/27/surveys/file2.txt");
        givenObject("root/prefix/20190102T17/28/refunds/file1.txt");

        whenStreamedDateWithHourThenMinuteFilters();

        thenReturned(
                "root/prefix/20181231T15/10/purchases/file.txt",
                "root/prefix/20181231T15/11/refunds/file.txt",
                "root/prefix/20181231T15/12/surveys/file.txt",

                "root/prefix/20190101T16/50/purchases/file.txt",
                "root/prefix/20190101T16/51/purchases/file1.txt",
                "root/prefix/20190101T16/51/refunds/file2.txt",

                "root/prefix/20190102T17/27/surveys/file1.txt",
                "root/prefix/20190102T17/27/surveys/file2.txt",
                "root/prefix/20190102T17/28/refunds/file1.txt");
    }

    @Test
    public void testFilteringNonTemporalField() {
        givenObject("root/prefix/20181231T09/5/purchases/file.txt");
        givenObject("root/prefix/20181231T09/6/purchases/file.txt");
        givenObject("root/prefix/20181231T09/7/refunds/file.txt");

        givenObject("root/prefix/20181231T15/10/purchases/file.txt");
        givenObject("root/prefix/20181231T15/11/refunds/file.txt");
        givenObject("root/prefix/20181231T15/12/surveys/file.txt");

        givenObject("root/prefix/20190101T16/50/purchases/file.txt");
        givenObject("root/prefix/20190101T16/51/purchases/file1.txt");
        givenObject("root/prefix/20190101T16/51/refunds/file2.txt");

        givenObject("root/prefix/20190102T17/27/surveys/file1.txt");
        givenObject("root/prefix/20190102T17/27/surveys/file2.txt");
        givenObject("root/prefix/20190102T17/28/refunds/file1.txt");

        whenStreamedNonTemporalField();

        thenReturned(
                "root/prefix/20181231T15/11/refunds/file.txt",
                "root/prefix/20181231T15/12/surveys/file.txt",

                "root/prefix/20190101T16/51/refunds/file2.txt",

                "root/prefix/20190102T17/27/surveys/file1.txt",
                "root/prefix/20190102T17/27/surveys/file2.txt",
                "root/prefix/20190102T17/28/refunds/file1.txt");
    }

    @Test
    public void testNoRootPrefix() {
        givenObject("20181231T09/5/purchases/file.txt");
        givenObject("20181231T09/6/purchases/file.txt");
        givenObject("20181231T09/7/refunds/file.txt");

        givenObject("20181231T15/10/purchases/file.txt");
        givenObject("20181231T15/11/refunds/file.txt");
        givenObject("20181231T15/12/surveys/file.txt");

        givenObject("20190101T16/50/purchases/file.txt");
        givenObject("20190101T16/51/purchases/file1.txt");
        givenObject("20190101T16/51/refunds/file2.txt");

        givenObject("20190102T17/27/surveys/file1.txt");
        givenObject("20190102T17/27/surveys/file2.txt");
        givenObject("20190102T17/28/refunds/file1.txt");

        whenStreamedNoRootPrefix();

        thenReturned(
                "20181231T15/11/refunds/file.txt",
                "20181231T15/12/surveys/file.txt",

                "20190101T16/51/refunds/file2.txt",

                "20190102T17/27/surveys/file1.txt",
                "20190102T17/27/surveys/file2.txt");
    }

    private void givenObject(String key) {
        client.putObject("bucket", key, key);
    }

    private void whenStreamedForDepth() {
        actual = new BucketRecursor(
                client,
                "bucket",
                '/',
                "root/prefix",
                10, // s3 mock library loops infinitely if the page size is too small
                1)
                .stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
    }

    private void whenStreamedDateThenHourFilters() {
        invoke("root/prefix",
                new Builder('/')
                        .after("2018-12-31T15:00:00Z")
                        .addLevelTemporal("'date='yyyyMMdd")
                        .addLevelTemporal("'hour='H")
                        .toFilters());
    }

    private void whenStreamedDateWithHourThenMinuteFilters() {
        invoke("root/prefix",
                new Builder('/')
                        .after("2018-12-31T15:10:30-05:00")
                        .addLevelTemporal("yyyyMMdd'T'HH")
                        .addLevelTemporal("mm")
                        .addLevelAnything()
                        .toFilters());
    }

    private void whenStreamedNonTemporalField() {
        invoke("root/prefix/",
                new Builder('/')
                        .after("2018-12-31T15:10:30-05:00[America/New_York]")
                        .addLevelTemporal("yyyyMMdd'T'HH")
                        .addLevelTemporal("mm")
                        .addLevelRegex("(refunds|surveys)")
                        .toFilters());
    }

    private void whenStreamedNoRootPrefix() {
        invoke(null,
                new Builder('/')
                        .between(
                                ZonedDateTime.parse("2018-12-31T15:10:30-05:00").toInstant(),
                                ZonedDateTime.parse("2019-01-02T17:28:00-05:00").toInstant(),
                                ZoneId.of("America/New_York"))
                        .addLevelTemporal("yyyyMMdd'T'HH")
                        .addLevelTemporal("mm")
                        .addLevelRegex("(refunds|surveys)")
                        .toFilters());
    }

    private void invoke(String root, List<Predicate<String>> filters) {
        actual = new BucketRecursor(
                client,
                "bucket",
                '/',
                root,
                10, // s3 mock library loops infinitely if the page size is too small
                filters)
                .stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
    }

    private void thenReturned(String... expected) {
        assertThat(actual, equalTo(Arrays.asList(expected)));
    }
}
