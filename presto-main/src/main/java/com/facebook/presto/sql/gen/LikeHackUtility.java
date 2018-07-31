package com.facebook.presto.sql.gen;

import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.RowExpression;
import io.airlift.joni.Matcher;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class LikeHackUtility
{
    LikeHackUtility() {}

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
                        (ce) -> isEqualRegexString(regexps, ce),
                        tautologyExpression());
                remapAllRecursively(
                        filter.get(),
                        (ce) -> isInWithRegexStrings(regexps, ce),
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

    private boolean isEqualRegexString(List<Regex> regexps, CallExpression ce)
    {
        return ce.getSignature().getName().equals("$operator$EQUAL")
                && ce.getArguments().stream().filter(arg -> arg instanceof ConstantExpression).anyMatch(
                    arg -> {
                        Object value = ((ConstantExpression) arg).getValue();
                        if (!(value instanceof Slice)) {
                            return false;
                        }
                        String stringValue = ((Slice) value).toStringUtf8();
                        for (Regex regex : regexps) {
                            if (isStringInRegex(stringValue, regex)) {
                                return true;
                            }
                        }
                        return false;
                    });
    }

    private boolean isInWithRegexStrings(List<Regex> regexps, CallExpression ce)
    {
        return ce.getSignature().getName().equals("IN")
                && ce.getArguments().stream().filter(arg -> arg instanceof ConstantExpression).allMatch(
                    arg -> {
                        Object value = ((ConstantExpression) arg).getValue();
                        if (!(value instanceof Slice)) {
                            return false;
                        }
                        String stringValue = ((Slice) value).toStringUtf8();
                        for (Regex regex : regexps) {
                            if (isStringInRegex(stringValue, regex)) {
                                return true;
                            }
                        }
                        return false;
                    });
    }

    private ConstantExpression tautologyExpression()
    {
        return constant(true, BOOLEAN);
    }

    public static final char UTF8_ESCAPE_SEPARATOR = 0x11;

    private boolean isStringInRegex(String stringValue, Regex regex)
    {
        String escapedValue = stringValue;
        // taking care of separator character prefix if exists
        if (stringValue.indexOf(UTF8_ESCAPE_SEPARATOR) != -1) {
            escapedValue = stringValue.substring(2);
        }
        Matcher matcher = regex.matcher(escapedValue.getBytes());
        int result = matcher.search(0, escapedValue.length(), Option.DEFAULT);
        return result != -1;
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
}
