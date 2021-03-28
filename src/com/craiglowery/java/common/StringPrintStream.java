package com.craiglowery.java.common;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public class StringPrintStream extends PrintStream {
    private static Charset UTF8 = Charset.availableCharsets().get("utf-8");
    {
        if (UTF8==null) {
            throw new Error("Could not find charset 'utf-8'");
        }
    }
    final ByteArrayOutputStream baos;
    public StringPrintStream(ByteArrayOutputStream ba) {
        super(ba, true, UTF8);
        baos=ba;
    }
    @Override
    public String toString() {
        return baos.toString();
    }
    public static StringPrintStream create() {
        return new StringPrintStream(new ByteArrayOutputStream());
    }
}
