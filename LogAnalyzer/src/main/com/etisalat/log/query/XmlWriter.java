package com.etisalat.log.query;

import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.config.LogConstants;
import com.etisalat.log.sort.SortField;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.apache.solr.common.util.XML;
import org.apache.solr.util.FastWriter;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.apache.solr.common.params.CommonParams.NAME;

public class XmlWriter {
    static final char[] indentChars = new char[81];

    static {
        Arrays.fill(indentChars, ' ');
        indentChars[0] = '\n';  // start with a newline
    }

    private Writer writer = null;
    private long realReturnNum;
    private boolean isClosed = false;
    private int level = 0;

    public XmlWriter(Writer writer, long realReturnNum) {
        this.realReturnNum = realReturnNum;
        this.writer = writer;
    }

    private static Writer getWriter(String filePath) throws IOException {
        return new FastWriter(new PrintWriter(new BufferedWriter(new FileWriter(filePath))));
    }

    protected static XmlWriter getWriter(String filePath, long num) throws IOException {
        return new XmlWriter(getWriter(filePath), num);
    }

    public int incLevel() {
        return ++level;
    }

    public int decLevel() {
        return --level;
    }

    private void writeAttr(String name, String val) throws IOException {
        writeAttr(name, val, true);
    }

    private void writeAttr(String name, String val, boolean escape) throws IOException {
        if (val != null) {
            writer.write(' ');
            writer.write(name);
            writer.write("=\"");
            if (escape) {
                XML.escapeAttributeValue(val, writer);
            } else {
                writer.write(val);
            }
            writer.write('"');
        }
    }

    private void indent() throws IOException {
        if (level <= 0) {
            return;
        }
        writer.write(indentChars, 0, Math.min((level << 1) + 1, indentChars.length));
    }

    private void startTag(String tag, String name, boolean closeTag) throws IOException {
        indent();
        writer.write('<');
        writer.write(tag);
        if (name != null) {
            writeAttr(NAME, name);
            if (closeTag) {
                writer.write("/>");
            } else {
                writer.write(">");
            }
        } else {
            if (closeTag) {
                writer.write("/>");
            } else {
                writer.write('>');
            }
        }
    }

    public void writeStart() throws IOException {
        writer.write(LogConstants.XML_HEADER);
        indent();
        writer.write("<result");
        writeAttr(NAME, "response");
        writeAttr("nums", Long.toString(realReturnNum));
        writer.write(">");
        incLevel();
    }

    public void writeEnd() throws IOException {
        decLevel();
        indent();
        writer.write("\n</result>");
        decLevel();
        indent();
        writer.write("\n</response>\n");
    }

    public void writeXmlJson(JsonObject jsonObject) throws IOException {
        startTag("doc", null, false);
        incLevel();

        Set<Map.Entry<String, JsonElement>> entrySet = jsonObject.entrySet();
        String fieldName = null;
        String val = null;
        SortField.Type type = null;
        for (Map.Entry<String, JsonElement> entry : entrySet) {
            fieldName = entry.getKey();
            val = entry.getValue() instanceof JsonNull ? null : entry.getValue().getAsString();
            type = LogConfFactory.columnQualifiersTypeMap.get(fieldName);
            if (val == null || type == null) {
                writeStr(fieldName, val == null ? "" : val, true);
                continue;
            }

            switch (type) {
            case double_n:
                writeDouble(fieldName, val);
                break;
            case float_n:
                writeFloat(fieldName, val);
                break;
            case integer:
                writeInt(fieldName, val);
                break;
            case long_n:
                writeLong(fieldName, val);
                break;
            case tdate:
                writeDate(fieldName, val);
            case string:
            case text_general:
                writeStr(fieldName, val, true);
                break;
            default:
                throw new RuntimeException("The type " + type + " does not support");
            }
        }

        decLevel();
        writer.write("\n</doc>");
    }

    private void writeNull(String name) throws IOException {
        writePrim("null", name, "", false);
    }

    private void writeStr(String name, String val, boolean escape) throws IOException {
        writePrim("str", name, val, escape);
    }

    private void writeInt(String name, String val) throws IOException {
        writePrim("int", name, val, false);
    }

    private void writeLong(String name, String val) throws IOException {
        writePrim("long", name, val, false);
    }

    private void writeFloat(String name, String val) throws IOException {
        writePrim("float", name, val, false);
    }

    private void writeDouble(String name, String val) throws IOException {
        writePrim("double", name, val, false);
    }

    private void writeDate(String name, String val) throws IOException {
        writePrim("date", name, val, false);
    }

    private void writePrim(String tag, String name, String val, boolean escape) throws IOException {
        int contentLen = val == null ? 0 : val.length();

        startTag(tag, name, contentLen == 0);
        if (contentLen == 0)
            return;

        if (escape) {
            XML.escapeCharData(val, writer);
        } else {
            writer.write(val, 0, contentLen);
        }

        writer.write('<');
        writer.write('/');
        writer.write(tag);
        writer.write('>');
    }

    public void flush() throws IOException {
        writer.flush();
    }

    public void close() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        isClosed = true;
    }

    public boolean isClosed() {
        return isClosed;
    }
}
