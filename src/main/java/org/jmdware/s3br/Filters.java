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

import static java.time.ZoneOffset.UTC;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility methods for building {@code List<Predicate<String>>}.
 *
 * @see Filters.Builder
 */
public final class Filters {

    private Filters() { }

    /**
     * Returns a prefix filter that tests the last element of prefix. Be sure to include a trailing
     * delimiter, which is how the s3 api returns prefixes.
     *
     * @param endsWith
     *         string that should appear at the end of the prefix. must end with the delimiter.
     *
     * @return filter predicate around {@link String#endsWith(String)}
     */
    public static Predicate<String> endsWith(String endsWith) {
        return (prefix) -> prefix.endsWith(endsWith);
    }

    /**
     * Returns a filter that accepts anything. Useful to recurse into any prefix at a given depth.
     *
     * @param <T>
     *         input type
     *
     * @return filter predicate that accepts anything
     */
    public static <T> Predicate<T> anything() {
        return (s) -> true;
    }

    /**
     * <p>
     * Helper for building predicates, prefix level by level, that walk s3 buckets prefix
     * hierarchies, depth first. Uses {@link DateTimeFormatter} to match on temporal prefixes and
     * {@link Pattern regular expression} to match the rest.
     * </p>
     *
     * <p>
     * For example:
     * </p>
     *
     * <pre>
     * activity-logs/
     *   date=20190415/
     *     hour=9/
     *       orders/
     *         orders1.gz
     *         orders2.gz
     *         ...
     *       messages/
     *         messages1.gz
     *         messages2.gz
     *         ...
     *     hour=10/
     *       orders/
     *         orders1.gz
     *         orders2.gz
     *         ...
     *       messages/
     *         messages1.gz
     *         messages2.gz
     *         ...
     *       ...
     *   date=20190416/
     *     hour=9/
     *       orders/
     *         ...
     *       messages/
     *         ...
     * </pre>
     *
     * <p>
     * If you only want the order objects for 20190415 hour 10 and later, build the temporal filters
     * like so:
     * </p>
     *
     * <pre>
     * List&lt;Predicate&lt;String&gt;&gt; filters = new Filters.Builder('/')
     *         .after("2019-04-15T10:00:00Z")
     *
     *         // add the DateTimeFormatter pattern for the date level in the prefix hierarchy
     *         .addLevelTemporal("'date='yyyyMMdd")
     *
     *         // add the DateTimeFormatter pattern for the hour level in the prefix hierarchy
     *         .addLevelTemporal("'hour='H")
     *
     *         // add a non-temporal regex pattern for the orders
     *         .addLevelRegex("orders")
     *
     *         .toFilters();
     * </pre>
     */
    public static class Builder {

        private static final Supported[] SUPPORTED_FIELDS = Supported.values();

        /**
         * Effectively forever.
         */
        private static final ZonedDateTime FOREVER = OffsetDateTime.MAX.toZonedDateTime();

        private final List<Predicate<List<String>>> filters = new ArrayList<>();

        private final List<Integer> temporalIndexes = new ArrayList<>();

        private final String delimiter;

        private final Pattern splitter;

        private ZonedDateTime after;

        private ZonedDateTime before;

        private ZoneId tz;

        private Predicate<List<String>> precedentFilter = anything();

        private DateTimeFormatter precedentFormatter;

        /**
         * Constructs a builder using the specified prefix delimiter.
         *
         * @param delimiter
         *         splits prefixes into levels
         */
        public Builder(char delimiter) {
            this.delimiter = Character.toString(delimiter);
            this.splitter = Delimiters.splitter(delimiter);
        }

        /**
         * Sets the specified zoned date time as the cutoff. Temporal values are parsed from the
         * prefixes in the same zone as the cutoff. For example:
         *
         * <ul>
         * <li>2018-12-31T20:10:30Z - utc</li>
         * <li>2018-12-31T15:10:30-05:00 - fixed offset from utc</li>
         * <li>2018-12-31T15:10:30-05:00[America/New_York] - use zone id when daylight savings matters</li>
         * </ul>
         *
         * @param zonedDateTime
         *         cutoff string defined per {@link ZonedDateTime#parse(CharSequence)}. inclusive.
         *         specifies time zone of temporal values parsed from the prefixes.
         *
         * @return this
         *
         * @see ZonedDateTime#parse(CharSequence)
         */
        public Builder after(String zonedDateTime) {
            return after(ZonedDateTime.parse(zonedDateTime));
        }

        /**
         * Sets instant as the cutoff. Temporal values are parsed from the prefixes in UTC.
         *
         * @param cutoff
         *         cutoff. inclusive. time zones will be parsed to utc.
         *
         * @return this
         */
        public Builder after(Instant cutoff) {
            return after(cutoff, UTC);
        }

        /**
         * Sets the specified instant as the cutoff. Temporal values are parsed from the prefixes in
         * specified zone.
         *
         * @param cutoff
         *         cutoff, inclusive.
         * @param tz
         *         time zone in which to parse temporal values from the prefixes
         *
         * @return this
         */
        public Builder after(Instant cutoff, ZoneId tz) {
            return after(cutoff.atZone(tz));
        }

        /**
         * Sets the specified zoned date time as the cutoff. Temporal values are parsed from the
         * prefixes in the same zone as the cutoff.
         *
         * @param cutoff
         *         cutoff, inclusive. specifies time zone of temporal values parsed from the
         *         prefixes.
         *
         * @return this
         */
        public Builder after(ZonedDateTime cutoff) {
            return between(cutoff, FOREVER);
        }

        /**
         * Sets the boundaries of allowed temporal values. Temporal values are parsed from the
         * prefixes in UTC.
         *
         * @param min
         *         minimum allowed point in time. inclusive.
         * @param max
         *         maximum point in time. exclusive.
         *
         * @return this
         */
        public Builder between(Instant min, Instant max) {
            return between(min, max, UTC);
        }

        /**
         * Sets the boundaries of allowed temporal values.
         *
         * @param min
         *         minimum allowed point in time. inclusive.
         * @param max
         *         maximum point in time. exclusive.
         * @param zone
         *         specifies time zone of temporal values parsed from the prefixes.
         *
         * @return this
         */
        public Builder between(Instant min, Instant max, ZoneId zone) {
            return between(min.atZone(zone), max.atZone(zone));
        }

        /**
         * Sets the boundaries of allowed temporal values. Temporal values are parsed from prefixes
         * in the same time zone as {@code minZdt}. Strings are formatted as {@link ZonedDateTime}.
         *
         * @param minZdt
         *         minimum allowed point in time. inclusive. specifies time zone of temporal values
         *         parsed from the prefixes.
         * @param maxZdt
         *         maximum point in time. exclusive.
         *
         * @return this
         *
         * @see ZonedDateTime#parse(CharSequence)
         */
        public Builder between(String minZdt, String maxZdt) {
            return between(ZonedDateTime.parse(minZdt), ZonedDateTime.parse(maxZdt));
        }

        /**
         * Sets the boundaries of allowed temporal values. Temporal values are parsed from prefixes
         * in the same time zone as {@code min}.
         *
         * @param min
         *         minimum allowed point in time. inclusive. specifies time zone of temporal values
         *         parsed from the prefixes.
         * @param max
         *         maximum point in time. exclusive.
         *
         * @return this
         */
        public Builder between(ZonedDateTime min, ZonedDateTime max) {
            checkState(this.after == null, "cutoff already set");

            if (min.isEqual(max) || min.isAfter(max)) {
                throw new IllegalArgumentException("min ('" + min + "') must be < max ('" + max + "')");
            }

            this.after = Objects.requireNonNull(min);
            this.before = Objects.requireNonNull(max);
            this.tz = min.getZone();

            return this;
        }

        /**
         * Adds a regex pattern for the next level in the prefix hierarchy. For example, to match
         * "purchases" <b>or</b> "messages", invoke {@code addLevelRegex("(purchase|messages)")}.
         *
         * @param regex
         *         regular expression pattern without leading or trailing delimiters
         *
         * @return this
         *
         * @see Pattern
         */
        public Builder addLevelRegex(String regex) {
            Pattern pattern = Pattern.compile(regex);

            int partIdx = filters.size();

            Predicate<List<String>> partsFilter = (parts) -> pattern.matcher(parts.get(partIdx)).matches();

            return addRelativePredicate(partsFilter);
        }

        /**
         * <p>
         * Adds a regex pattern for the next level in the prefix hierarchy. The argument to the
         * regular expression will be the <b>entire</b> prefix (relative to the start prefix). This
         * is useful for patterns that contain back references to earlier levels in the prefix
         * hierarchy.
         * </p>
         *
         * <p>
         * For example, to match prefixes where "src" and "dst" are the same:
         * </p>
         *
         * <pre>
         * // matches "dt=20194019/h=23/src=foo/dst=foo/"
         * ...
         * .addLevelTemporal('dt='yyyyMMdd")           // depth 1: date
         * .addLevelTemporal('h='H")                   // depth 2: h
         * .addLevelAnything()                         // depth 3: src - anything so we can compare in the next level
         * .addRelativeRegex(".*&#47;src=(\\w+)&#47;dst=\\1/") // depth 4: dst vs src
         * </pre>
         *
         * <p>
         * And to match prefixes where "src" and "dst" are different:
         * </p>
         *
         * <pre>
         * // matches "dt=20190420/h=4/src=foo/dst=bar/"
         * ...
         * .addLevelTemporal('dt='yyyyMMdd")
         * .addLevelTemporal('h='H")
         * .addLevelAnything()
         * .addRelativeRegex(".*&#47;src=(\\w+)&#47;dst=(?!\\1).+/")
         * </pre>
         *
         * @param regex
         *         regular expression pattern for entire prefix relative to the start prefix. be
         *         sure to account for the trailing delimiter, which is how the s3 api returns
         *         prefixes.
         *
         * @return this
         */
        public Builder addRelativeRegex(String regex) {
            Pattern pattern = Pattern.compile(regex);

            return addRelativePredicate((parts) -> {
                String joined = parts.stream().collect(Collectors.joining(delimiter, "", delimiter));

                return pattern.matcher(joined).matches();
            });
        }

        /**
         * <p>
         * Adds a {@link DateTimeFormatter} pattern for the next level in the prefix hierarchy.
         * </p>
         *
         * <p>
         * For example, to match:
         * </p>
         *
         * <pre>
         * dt=2019-04-19
         * </pre>
         *
         * <p>
         * Invoke {@code addLevelTemporal("'dt='yyyy'-'MM'-'dd")}.
         * </p>
         *
         * <p>
         * Temporal matching rounds "wide". If the cutoff is 2019-04-19T15:30:00Z and the prefix
         * ends with "dt=2019-04-19", that prefix will be recursively listed since it may contain
         * relevant child prefixes. As the subsequent prefixes become more granular, the amount of
         * rounding decreases, narrowing the range of allowed values. Compared to
         * 2019-04-19T15:30:00Z, there could be several hours of extra data in "dt=2019-04-19". But
         * there will only be 30 minutes of extra data in "dt=2019-04-19/h=15".
         * </p>
         *
         * @param pattern
         *         {@link DateTimeFormatter} pattern without leading or trailing delimiters
         *
         * @return this
         *
         * @see DateTimeFormatter
         */
        public Builder addLevelTemporal(String pattern) {
            DateTimeFormatter partFormatter = DateTimeFormatter.ofPattern(pattern).withZone(tz);
            DateTimeFormatter prefixFormatter;

            if (precedentFormatter == null) {
                prefixFormatter = partFormatter;
            } else {
                prefixFormatter = new DateTimeFormatterBuilder()
                        .append(precedentFormatter)
                        .appendLiteral(" ")
                        .append(partFormatter)
                        .toFormatter()
                        .withZone(tz);
            }

            int partIdx = filters.size();

            temporalIndexes.add(partIdx);

            // derive the cutoff for the granularity we have so far
            ZonedDateTime prefixCutoff = prefixCutoff(prefixFormatter);

            Predicate<List<String>> partsFilter;

            if (prefixCutoff == null) {
                // not enough temporal patterns yet to parse anything to compare
                partsFilter = anything();
            } else {
                List<Integer> indexes = new ArrayList<>(temporalIndexes);

                partsFilter = (parts) -> {
                    // gather the year, month, day, etc. in one place.
                    String text = indexes.stream()
                            .map(parts::get)
                            .collect(Collectors.joining(" "));

                    ZonedDateTime zdt = parse(prefixFormatter, text);

                    return zdt != null
                            && (zdt.isAfter(prefixCutoff) || zdt.equals(prefixCutoff))
                            && (zdt.isBefore(before));
                };
            }

            addRelativePredicate(partsFilter);

            precedentFormatter = prefixFormatter;

            return this;
        }

        private ZonedDateTime parse(DateTimeFormatter formatter, String text) {
            TemporalAccessor temporal;

            try {
                temporal = formatter.parse(text);
            } catch (RuntimeException e) {
                return null;
            }

            int lastIndex = IntStream.range(0, SUPPORTED_FIELDS.length)
                    .filter((i) -> !temporal.isSupported(SUPPORTED_FIELDS[i].chronoField))
                    .findFirst()
                    .orElse(SUPPORTED_FIELDS.length);

            if (lastIndex == 0) {
                return null;
            }

            ZonedDateTime zdt = Instant.EPOCH.atZone(tz);

            for (int i = 0; i < lastIndex; i++) {
                zdt = SUPPORTED_FIELDS[i].adjust(temporal, zdt);
            }

            return zdt;
        }

        private ZonedDateTime prefixCutoff(DateTimeFormatter prefixFormatter) {
            return parse(prefixFormatter, prefixFormatter.format(after));
        }

        /**
         * Adds a test for the next level in the prefix hierarchy that tests the entire prefix
         * going back to but not including the start prefix. Recursion will continue into prefixes
         * that test true. For "start/prefix1/prefix2/prefix3/" where "start/" is the starting
         * prefix, the argument to the test will be {@code ["prefix1" "prefix2" "prefix3"]} (the
         * start prefix is <b>not</b> included).
         *
         * @param test
         *         test for the relative (to the start) prefix, split on the delimiter
         *
         * @return this
         */
        public Builder addRelativePredicate(Predicate<List<String>> test) {
            Predicate<List<String>> prefixFilter = precedentFilter.and(test);

            filters.add(lengthCheck(filters.size() + 1).and(prefixFilter));
            precedentFilter = prefixFilter;

            return this;
        }

        /**
         * <p>
         * Adds a pattern for a prefix level that matches anything. Effectively {@code ".+"}. This is
         * useful when a there is a prefix level between then temporal prefix and the object. For
         * example, in the s3 object key:
         * </p>
         *
         * <pre>
         * some-prefix/date=20190416/hour=3/purchases/purchase.log.txt.gz
         * some-prefix/date=20190416/hour=3/charge-backs/charge-backs.log.txt.gz
         * </pre>
         *
         * <p>
         * "purchases" is the prefix level between the "hour=3" and the s3 object "purchase.log.txt.gz"
         * and "charge-backs" is between "hour=3" and "charge-backs.log.txt.gz".
         * </p>
         *
         * <p>
         * To target any s3 object one prefix below "hour=3", use this method.
         * </p>
         *
         * <pre>
         * ...
         * .addLevelTemporal("'hour='H")
         * .addLevelAnything()           // true for "purchases", "charge-backs", or anything else
         * .toFilters();
         * </pre>
         *
         * @return this
         */
        public Builder addLevelAnything() {
            return addLevelRegex(".+");
        }

        /**
         * Returns a list of predicates that walk a prefix hierarchy.
         *
         * @return filters
         */
        public List<Predicate<String>> toFilters() {
            if (precedentFormatter != null) {
                if (prefixCutoff(precedentFormatter) == null) {
                    // too little to parse anything to compare with the cutoff
                    throw new IllegalStateException("too few temporal patterns");
                }
            }

            return filters.stream()
                    .map((partsFilter) -> (Predicate<String>) (prefix) -> partsFilter.test(
                            Arrays.asList(splitter.split(prefix))))
                    .collect(Collectors.toList());
        }

        private static void checkState(boolean state, String msg) {
            if (!state) {
                throw new IllegalStateException(msg);
            }
        }

        /**
         * Returns a filter that performs array length check. Use to chain with other logic:
         *
         * <pre>
         * Predicate&lt;List<String>&gt; filter = lengthCheck(i).and((elements) -> ...);
         * </pre>
         *
         * @param minLength
         * minimum allowed length of the prefix elements array
         * @return length checking filter
         */
        private static Predicate<List<String>> lengthCheck(int minLength) {
            return (elements -> elements.size() >= minLength);
        }

        @SuppressWarnings("unused")
        private enum Supported {
            YEAR(ChronoField.YEAR),
            MONTH(ChronoField.MONTH_OF_YEAR),
            DAY(ChronoField.DAY_OF_MONTH),
            HOUR(ChronoField.HOUR_OF_DAY),
            MINUTE(ChronoField.MINUTE_OF_HOUR),
            SECOND(ChronoField.SECOND_OF_MINUTE),
            FRACTIONAL_SECOND(ChronoField.MILLI_OF_SECOND) {
                @Override
                ZonedDateTime adjust(TemporalAccessor temporal, ZonedDateTime base) {
                    if (temporal.isSupported(ChronoField.NANO_OF_SECOND)) {
                        return base.with(ChronoField.NANO_OF_SECOND, temporal.get(ChronoField.NANO_OF_SECOND));
                    }

                    if (temporal.isSupported(ChronoField.MILLI_OF_SECOND)) {
                        return base.with(ChronoField.MILLI_OF_SECOND, temporal.get(ChronoField.MILLI_OF_SECOND));
                    }

                    return base;
                }
            };

            private final ChronoField chronoField;

            Supported(ChronoField chronoField) {
                this.chronoField = chronoField;
            }

            ZonedDateTime adjust(TemporalAccessor temporal, ZonedDateTime base) {
                if (temporal.isSupported(chronoField)) {
                    return base.with(chronoField, temporal.get(chronoField));
                }

                return base;
            }
        }
    }
}
