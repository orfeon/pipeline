package com.mercari.solution.util.domain.text.template;

import java.time.LocalDate;
import java.util.UUID;

public class StringFunctions {

    public String format(String format, Object... args) {
        return String.format(format, args);
    }

    public String uuid() {
        return UUID.randomUUID().toString();
    }

    public String replaceAll(String str, String from, String to) {
        if(str == null || from == null || to == null) {
            return str;
        }
        return str.replaceAll(from, to);
    }

    public String replaceAll(LocalDate date, String from, String to) {
        if(date == null) {
            return null;
        }
        if(from == null || to == null) {
            return date.toString();
        }
        return date.toString().replaceAll(from, to);
    }

    public String replaceAll(org.joda.time.Instant timestamp, String from, String to) {
        if(timestamp == null) {
            return null;
        }
        if(from == null || to == null) {
            return timestamp.toString();
        }
        return timestamp.toString().replaceAll(from, to);
    }

    public String reverse(String text) {
        if(text == null) {
            return "";
        }
        return new StringBuilder(text).reverse().toString();
    }

}
