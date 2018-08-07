package com.facebook.presto.sql.gen;

import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.RowExpression;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class LikeHackUtility
{
    public LikeHackUtility() {}

    /**
     * Removes String filters added during push-downing like expressions.
     * (like predicate is transformed into LIKE AND EQUALS expression.
     * @param filter filter to modify (value will be changed)
     * @return modified filter
     */
    Optional<RowExpression> removeFakeStringFilters(Optional<RowExpression> filter)
    {
        if (filter.isPresent()) {
            List<Regex> regexps = fetchLikeRegexps(filter.get());

            if (!regexps.isEmpty()) {
                remapAllRecursively(
                        filter.get(),
                        this::isEqualLikeString,
                        tautologyExpression());
                remapAllRecursively(
                        filter.get(),
                        this::isInWithLikeStrings,
                        tautologyExpression());
            }
        }
        return filter;
    }

    private <T> List<T> findAllRecursively(RowExpression filter, Function<CallExpression, Boolean> childFilter, Function<CallExpression, T> extractResult)
    {
        RecursiveResult result = findRecursively(filter, childFilter, Optional.empty());
        return result.foundEntries.stream().map(extractResult).collect(Collectors.toList());
    }

    private void remapAllRecursively(RowExpression filter, Function<CallExpression, Boolean> childFilter, RowExpression newFilter)
    {
        findRecursively(filter, childFilter, Optional.of(newFilter));
    }

    private RecursiveResult findRecursively(RowExpression filter, Function<CallExpression, Boolean> childFilter, Optional<RowExpression> newFilter)
    {
        if (filter instanceof CallExpression) {
            CallExpression ce = (CallExpression) filter;
            if (childFilter.apply(ce)) {
                return new RecursiveResult(true, false, ce);
            }
            ArrayList<RowExpression> mutArguments = new ArrayList<>(ce.getArguments());

            RecursiveResult result = new RecursiveResult(false, false);

            for (int i = 0; i < ce.getArguments().size(); i++) {
                RecursiveResult childRecursiveResult = findRecursively(ce.getArguments().get(i), childFilter, newFilter);
                if (childRecursiveResult.found) {
                    result.found = true;
                    result.foundEntries.addAll(childRecursiveResult.foundEntries);

                    if (!childRecursiveResult.swapped && newFilter.isPresent()) {
                        mutArguments.set(i, newFilter.get());

                        ce.setArguments(mutArguments);
                        result.swapped = true;
                    }
                }
            }
            return result;
        }
        return new RecursiveResult(false, false);
    }

    private List<Regex> fetchLikeRegexps(RowExpression filter)
    {
        return findAllRecursively(
                filter,
                (ce) -> ce.getSignature().getName().equals("LIKE"),
                (ce) -> ce.getArguments().stream()
                        .filter(arg -> arg instanceof ConstantExpression
                                && ((ConstantExpression) arg).getValue() instanceof Regex)
                        .map(arg -> ((Regex) ((ConstantExpression) arg).getValue()))
                        .findFirst()
        ).stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    private boolean isStringLikePrefixed(String string)
    {
        if (string.isEmpty()) {
            return false;
        }
        for (char specialChar : StringMetaCharacter.getAllCharacters()) {
            if (specialChar == string.charAt(0)) {
                return true;
            }
        }
        return false;
    }

    private boolean isEqualLikeString(CallExpression ce)
    {
        return ce.getSignature().getName().equals("$operator$EQUAL")
                && ce.getArguments().stream().filter(arg -> arg instanceof ConstantExpression).anyMatch(
                    arg -> {
                        Object value = ((ConstantExpression) arg).getValue();
                        if (!(value instanceof Slice)) {
                            return false;
                        }
                        String stringValue = ((Slice) value).toStringUtf8();
                        return isStringLikePrefixed(stringValue);
                    });
    }

    private boolean isInWithLikeStrings(CallExpression ce)
    {
        return ce.getSignature().getName().equals("IN")
                && ce.getArguments().stream().filter(arg -> arg instanceof ConstantExpression).allMatch(
                    arg -> {
                        Object value = ((ConstantExpression) arg).getValue();
                        if (!(value instanceof Slice)) {
                            return false;
                        }
                        String stringValue = ((Slice) value).toStringUtf8();
                        return isStringLikePrefixed(stringValue);
                    });
    }

    private ConstantExpression tautologyExpression()
    {
        return constant(true, BOOLEAN);
    }

    public enum StringMetaCharacter
    {
        STARTS_WITH((char) 0x11),
        ENDS_WITH((char) 0x12),
        CONTAINS((char) 0x13);

        private char unicodeCharacter;

        StringMetaCharacter(char unicodeCharacter)
        {
            this.unicodeCharacter = unicodeCharacter;
        }

        public char getCharacter()
        {
            return unicodeCharacter;
        }

        public static char[] getAllCharacters()
        {
            StringMetaCharacter[] enumValues = StringMetaCharacter.class.getEnumConstants();
            char[] characters = new char[enumValues.length];
            for (int i = 0; i < enumValues.length; i++) {
                characters[i] = enumValues[i].getCharacter();
            }
            return characters;
        }
    }

    private class RecursiveResult
    {
        boolean found;
        boolean swapped;
        List<CallExpression> foundEntries = new ArrayList<>();

        RecursiveResult(boolean found, boolean swapped)
        {
            this.found = found;
            this.swapped = swapped;
        }

        RecursiveResult(boolean found, boolean swapped, CallExpression entry)
        {
            this(found, swapped);
            addEntry(entry);
        }

        void addEntry(CallExpression entry)
        {
            this.foundEntries.add(entry);
        }
    }

    private static final String WILDCARD = "%";

    /**
     * Splits like filter value string into list of strings basing on wildcard presence.
     * Output strings are preceded with special character ({@see SingleMetaCharacter} if it's not exact match.
     *
     * e.g. "%asd%%q\%we" where escape sign is "\" is split to ["asd", "q\%we"], where "asd" is preceded with {@link StringMetaCharacter#CONTAINS} sign
     * and "q\%we" is preceded with {@link StringMetaCharacter#ENDS_WITH} one.
     *
     * If string without wildcard is provided, the same string is returned in one-element list.
     * Multiple wildcards after each other reduce to single one.
     *
     * @param string string to analyze
     * @param escapeChar optional escape char for given filter
     * @return List of strings preceded by {@link StringMetaCharacter} character.
     */
    public List<String> splitLikeStrings(String string, Optional<Character> escapeChar)
    {
        if (string.isEmpty()) {
            return Collections.singletonList(string);
        }

        String wildcardRegex;
        wildcardRegex = escapeChar
                .map(character -> {
                    String escapedEscape = Pattern.quote(String.valueOf(character));
                    // match 1 or more wildcards but not preceded by escape char
                    return "(?<!" + escapedEscape + "{1,1})" + WILDCARD + "+";
                })
                .orElse(WILDCARD + "+");

        List<String> splitLikeStrings = Arrays.asList(string.split(wildcardRegex, -1));

        if (splitLikeStrings.stream().allMatch(String::isEmpty)) {
            return Collections.singletonList(String.valueOf(StringMetaCharacter.CONTAINS.getCharacter()));
        }

        if (splitLikeStrings.size() == 1) {
            return Collections.singletonList(splitLikeStrings.get(0));
        }

        List<WildcardedString> wildcardedStrings = new ArrayList<>();
        for (int i = 1; i < splitLikeStrings.size() - 1; i++) {
            WildcardedString wildcardedString = new WildcardedString(splitLikeStrings.get(i), StringMetaCharacter.CONTAINS);
            wildcardedStrings.add(wildcardedString);
        }

        if (!splitLikeStrings.get(0).isEmpty()) {
            WildcardedString wildcardedString = new WildcardedString(splitLikeStrings.get(0), StringMetaCharacter.STARTS_WITH);
            wildcardedStrings.add(0, wildcardedString);
        }

        if (!splitLikeStrings.get(splitLikeStrings.size() - 1).isEmpty()) {
            WildcardedString wildcardedString = new WildcardedString(splitLikeStrings.get(splitLikeStrings.size() - 1), StringMetaCharacter.ENDS_WITH);
            wildcardedStrings.add(wildcardedString);
        }

        return wildcardedStrings.stream().map(ws -> ws.getWildcard().getCharacter() + ws.getStringValue()).collect(Collectors.toList());
    }

    private class WildcardedString
    {
        private String stringValue;
        private StringMetaCharacter wildcard;

        private WildcardedString(String stringValue, StringMetaCharacter wildcard)
        {
            this.stringValue = stringValue;
            this.wildcard = wildcard;
        }

        private String getStringValue()
        {
            return stringValue;
        }

        private StringMetaCharacter getWildcard()
        {
            return wildcard;
        }
    }
}
