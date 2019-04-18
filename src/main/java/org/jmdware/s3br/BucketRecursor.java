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

import static org.jmdware.s3br.Delimiters.withTrailingDelimiter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * <p>
 * Lazily and recursively lists the prefixes in a s3 bucket to a specific depth and streams the s3
 * object summaries from the prefixes that satisfy the filter criteria. Laziness allows traversal of
 * wide and deep prefix hierarchies with a small memory footprint, even for prefixes that contain
 * millions of s3 objects.
 * </p>
 *
 * <p>
 * For example, consider s3 objects prefixed with dates and hours:
 * </p>
 *
 * <pre>
 * activity-logs/
 *     date=20190415/
 *         hour=22/
 *           ...
 *         hour=23/
 *             orders/
 *                 orders1.gz
 *                 orders2.gz
 *                 ...
 *             views/
 *                 ...
 *     date=20190416/
 *         hour=1/
 *             orders/
 *                 ...
 *             ...
 *         ...
 *     ...
 * </pre>
 *
 * <p>
 * Starting with "activity-logs" as the "start" prefix, recursion evaluates the <b>prefixes</b> in
 * the following order (prefixes are relative to the "start"):
 * </p>
 *
 * <ol>
 *     <li>date=20190415/</li>
 *     <li>date=20190415/hour=23/</li>
 *     <li>date=20190415/hour=23/orders/</li>
 *     <li>date=20190415/hour=23/views/</li>
 *     <li>date=20190416/</li>
 *     <li>date=20190416/hour=0/</li>
 *     <li>date=20190416/hour=0/orders/</li>
 *     <li>date=20190416/hour=0/views/</li>
 *     <li>etc.</li>
 * </ol>
 *
 * <p>
 * The easiest way to find all order and views objects is to recurse to prefix depth 3.
 * </p>
 *
 * <pre>
 * BucketRecursor recursor = new BucketRecursor(
 *     amazonS3,
 *     bucket,
 *     '/',              // prefix delimiter
 *     "activity-logs",  // start prefix where recursion begins
 *     pageSize,         // max results per call to s3 list objects api
 *     3);               // prefix depth
 *
 * Stream&lt;S3ObjectSummary&gt; summaries = recursor.stream();
 *
 * summaries.forEach((summary) -&gt; downloader::download);
 * </pre>
 *
 * <p>
 * The easiest way to find all order objects from 2019-04-15 hour 23 and after is to use
 * {@link Filters.Builder}, which generates the required three filters, three because
 * the "order" data are three prefixes below the start prefix. Each filter is a
 * {@code Predicate<String>}. The argument to the filter at each depth is the <b>relative</b>
 * prefix (without the start prefix in front).
 * </p>
 *
 * <ol>
 *     <li>"date=&lt;yyyyMMdd&gt;/" where &lt;yyyyMMdd&gt; is 2015-04-15 or later</li>
 *     <li>"date=&lt;yyyyMMdd&gt;/hour=&lt;H&gt;/" where &lt;yyyyMMdd H&gt; is
 *     2015-04-15T23 or later.</li>
 *     <li>"date=&lt;yyyyMMdd&gt;/hour=&lt;H&gt;/orders/" where &lt;yyyyMMdd H&gt;
 *     is 2015-04-15T23 or later.</li>
 * </ol>
 *
 * <p>
 * Filter 1 (counting from 1) is only invoked at depth 1, filter 2 at depth 2, and filter 3 at
 * depth 3. Filter n is only ever invoked at depth n.
 * </p>
 *
 * <p>
 * Rather than implement the date parsing, use {@link Filters.Builder}, which accepts a
 * cutoff time and date time formatter patterns for extracting temporal data from prefixes.
 * </p>
 *
 * @see Filters.Builder
 */
public class BucketRecursor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketRecursor.class);

    private final AmazonS3 client;

    private final String delimiter;

    private final String bucket;

    private final String startPrefix;

    private final int startPrefixLength;

    private final int pageSize;

    private final List<Predicate<String>> filters;

    /**
     * Constructs a recursor that finds all objects beneath prefixes at the specified depth.
     *
     * @param client
     *         s3 client
     * @param bucket
     *         bucket
     * @param delimiter
     *         prefix delimiter
     * @param startPrefix
     *         prefix to begin recursion, depth 0
     * @param pageSize
     *         max results per call to s3 list api
     * @param prefixDepth
     *         number of prefix levels to recurse
     *
     * @see Filters#anything()
     */
    public BucketRecursor(
            AmazonS3 client,
            String bucket,
            char delimiter,
            @Nullable String startPrefix,
            int pageSize,
            int prefixDepth) {
        this(client, bucket, delimiter, startPrefix, pageSize, Stream.generate(Filters::<String> anything)
                .limit(prefixDepth)
                .collect(Collectors.toList()));
    }

    /**
     * Constructs a recursor that finds all objects beneath prefixes at the specified depth.
     *
     * @param client
     *         s3 client
     * @param bucket
     *         bucket
     * @param delimiter
     *         prefix delimiter
     * @param startPrefix
     *         prefix to begin recursion, depth 0
     * @param pageSize
     *         max results per call to s3 list api
     * @param prefixFilters
     *         filters that match each level of prefix. each filter is given the <b>relative</b>
     *         prefix. number of filters determines recursion depth. prefix at depth 1 is passed to
     *         first filter, at depth 2 to second, at depth 3 to third, etc. predicates return
     *         {@code true} to indicate require should continue.
     */
    @SafeVarargs
    public BucketRecursor(
            AmazonS3 client,
            String bucket,
            char delimiter,
            @Nullable String startPrefix,
            int pageSize,
            Predicate<String>... prefixFilters) {
        this(client, bucket, delimiter, startPrefix, pageSize, Arrays.asList(prefixFilters));
    }

    /**
     * Constructs a recursor that finds all objects beneath prefixes at the specified depth.
     *
     * @param client
     *         s3 client
     * @param bucket
     *         bucket
     * @param delimiter
     *         prefix delimiter
     * @param startPrefix
     *         prefix to begin recursion, depth 0
     * @param pageSize
     *         max results per call to s3 list api
     * @param prefixFilters
     *         filters that match each level of prefix. each filter is given the <b>relative</b>
     *         prefix. number of filters determines recursion depth. prefix at depth 1 is passed to
     *         first filter, at depth 2 to second, at depth 3 to third, etc. predicates return
     *         {@code true} to indicate require should continue.
     */
    public BucketRecursor(
            AmazonS3 client,
            String bucket,
            char delimiter,
            @Nullable String startPrefix,
            int pageSize,
            List<Predicate<String>> prefixFilters) {
        this.client = Objects.requireNonNull(client);
        this.delimiter = Character.toString(delimiter);
        this.bucket = Objects.requireNonNull(bucket);

        if (startPrefix == null) {
            this.startPrefix = null;
            this.startPrefixLength = 0;
        } else {
            this.startPrefix = withTrailingDelimiter(delimiter, startPrefix);
            this.startPrefixLength = this.startPrefix.length();
        }

        if (pageSize < 1) {
            throw new IllegalArgumentException("page size must be > 0: " + pageSize);
        }
        this.pageSize = pageSize;

        this.filters = Objects.requireNonNull(prefixFilters);
    }

    /**
     * Lazily invokes the s3 list api to query prefixes and keys and returns the results in a
     * stream. The returned stream is not thread safe and should be accessed by one thread at a
     * time.
     *
     * @return stream of s3 object summaries
     */
    public Stream<S3ObjectSummary> stream() {
        LOGGER.info("recursively listing '{}' to prefix depth {}", startPrefix, filters.size());

        Stream<String> stream = Stream.of(startPrefix);

        for (int i = 0; i < filters.size(); i++) {
            int filterIdx = i;
            stream = stream.flatMap((prefix) -> listPrefixes(prefix, filterIdx));
        }

        return stream.flatMap(this::listSummaries);
    }

    private Stream<String> listPrefixes(String prefix, int filterIdx) {
        LOGGER.info("listing prefixes in '{}' (depth {})", prefix, filterIdx);

        Iterator<String> iter = new S3Iterator<>(prefix, ListObjectsV2Result::getCommonPrefixes);

        // Modeled after BufferReader.lines()
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iter, Spliterator.ORDERED | Spliterator.NONNULL), false)
                .filter((p) -> filters.get(filterIdx).test(p.substring(startPrefixLength)));
    }

    private Stream<S3ObjectSummary> listSummaries(String prefix) {
        LOGGER.info("listing keys in '{}' (depth {})", prefix, filters.size());

        Iterator<S3ObjectSummary> iter = new S3Iterator<>(prefix, ListObjectsV2Result::getObjectSummaries);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iter, Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    private ListObjectsV2Request newRequest(String prefix) {
        return new ListObjectsV2Request()
                .withBucketName(bucket)
                .withDelimiter(delimiter)
                .withPrefix(prefix)
                .withMaxKeys(pageSize);
    }

    private class S3Iterator<T> implements Iterator<T> {

        private final ListObjectsV2Request request;

        private final Function<ListObjectsV2Result, Iterable<T>> accessor;

        private Iterator<T> delegate;

        private String token;

        private S3Iterator(String prefix, Function<ListObjectsV2Result, Iterable<T>> accessor) {
            this.request = newRequest(prefix);
            this.accessor = Objects.requireNonNull(accessor);
        }

        @Override
        public boolean hasNext() {
            if (delegate == null) {
                nextPage();
            }

            if (!delegate.hasNext() && token != null) {
                nextPage();
            }

            return delegate.hasNext();
        }

        private void nextPage() {
            if (token != null) {
                request.setContinuationToken(token);
            }

            ListObjectsV2Result result = client.listObjectsV2(request);

            delegate = accessor.apply(result).iterator();

            if (result.isTruncated()) {
                token = result.getNextContinuationToken();
            } else {
                token = null;
            }
        }

        @Override
        public T next() {
            return delegate.next();
        }
    }
}
