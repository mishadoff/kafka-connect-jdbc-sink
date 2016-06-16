package com.mishadoff.connector.utils;

import java.util.List;
import java.util.stream.Collectors;

public final class StringUtils {

    public static String fieldsClause(List<String> fields) {
        return "(" + fields.stream().collect(Collectors.joining(",")) + ")";
    }

    public static String placeholderClause(List<String> fields) {
        return "(" + fields.stream().map(e -> "?").collect(Collectors.joining(",")) + ")";
    }

}
