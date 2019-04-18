package org.jmdware.s3br;

import java.util.regex.Pattern;

final class Delimiters {

    private Delimiters() { }

    static String withTrailingDelimiter(char delimiter, String fragment) {
        if (fragment.charAt(fragment.length() - 1) == delimiter) {
            return fragment;
        }

        return fragment.concat(Character.toString(delimiter));
    }

    static Pattern splitter(char delimiter) {
        return Pattern.compile(Pattern.quote(Character.toString(delimiter)));
    }
}
