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

    /** Hex SHA-256 of the UTF-8 text (e.g. deterministic idempotency keys). */
    public String sha256(String text) {
        return digest("SHA-256", text);
    }

    public String md5(String text) {
        return digest("MD5", text);
    }

    /** Hex HMAC-SHA256 of the UTF-8 text with the given secret (webhook signature headers). */
    public String hmacSha256(String text, String secret) {
        return hmac("HmacSHA256", text, secret);
    }

    public String hmacSha1(String text, String secret) {
        return hmac("HmacSHA1", text, secret);
    }

    public String base64(String text) {
        if(text == null) {
            return null;
        }
        return java.util.Base64.getEncoder().encodeToString(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String digest(final String algorithm, final String text) {
        if(text == null) {
            return null;
        }
        try {
            final java.security.MessageDigest md = java.security.MessageDigest.getInstance(algorithm);
            return hex(md.digest(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (final java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String hmac(final String algorithm, final String text, final String secret) {
        if(text == null || secret == null) {
            return null;
        }
        try {
            final javax.crypto.Mac mac = javax.crypto.Mac.getInstance(algorithm);
            mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(java.nio.charset.StandardCharsets.UTF_8), algorithm));
            return hex(mac.doFinal(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (final java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String hex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for(final byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

}
