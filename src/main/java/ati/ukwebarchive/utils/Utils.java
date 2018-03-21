/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * Some utility methods
 * 
 * @author pierpaolo
 */
public class Utils {

    /**
     * Loads a file in a set. Each row in the file is an entry in the set.
     * 
     * @param file The input file
     * @param filterComment Filter rows that start with the char '#'
     * @return The set of strings
     * @throws IOException
     */
    public static Set<String> loadFileInSet(File file, boolean filterComment) throws IOException {
        Set<String> set = new HashSet<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        while (reader.ready()) {
            String obj = reader.readLine().trim();
            if (filterComment) {
                if (!obj.startsWith("#")) {
                    set.add(obj);
                }
            } else {
                set.add(obj);
            }
        }
        reader.close();
        return set;
    }

    /**
     * Format the date form warc/arc record in YYYYMM
     * @param dateString
     * @return
     */
    public static String getYearMonthFromDateStr(String dateString) {
        return dateString.substring(0, 6);
    }

    /**
     *
     * @param statistics
     * @param file
     * @throws IOException
     */
    @Deprecated
    public static void saveStatistics(Map<String, Long> statistics, File file) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        List<String> list = new ArrayList<>(statistics.keySet());
        Collections.sort(list);
        for (String key : list) {
            writer.append(key + "\t" + statistics.get(key).toString());
            writer.newLine();
        }
        writer.close();
    }

    /**
     *
     * @param statistics
     */
    @Deprecated
    public static void printStatistics(Map<String, Long> statistics) {
        System.out.println();
        List<String> list = new ArrayList<>(statistics.keySet());
        Collections.sort(list);
        for (String key : list) {
            System.out.println(key + "\t" + statistics.get(key).toString());
        }
    }

    /**
     * Returns a list of tokens from a reader. The text is tokenized by the analyzer 
     * @param analyzer The Analyzer used to tokenize the text
     * @param fieldname The name of the field
     * @param reader The input reader
     * @return The list of tokens
     * @throws IOException
     */
    public static List<String> getTokens(Analyzer analyzer, String fieldname, Reader reader) throws IOException {
        List<String> tokens = new ArrayList<>();
        TokenStream tokenStream = analyzer.tokenStream(fieldname, reader);
        tokenStream.reset();
        CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
        while (tokenStream.incrementToken()) {
            String token = cattr.toString();
            tokens.add(token);
        }
        tokenStream.end();
        tokenStream.close();
        return tokens;
    }

    /**
     * Extracts the text from html pages by using Jsoup library
     * @param file The input file
     * @return The text
     * @throws IOException
     */
    public static String getContent(File file) throws IOException {
        Document doc = Jsoup.parse(file, null);
        return doc.body().text();
    }

    /**
     * Extracts the text from html pages by using Jsoup library
     * @param stream The input stream
     * @return The text
     * @throws IOException
     */
    public static String getContent(InputStream stream) throws IOException {
        Document doc = Jsoup.parse(stream, null, "");
        return doc.body().text();
    }

    /**
     * Return the base mime content type
     * @param contentType The input content type
     * @return The mime content type
     */
    public static String getBaseContentType(String contentType) {
        return contentType.split(";")[0].trim();
    }

}
