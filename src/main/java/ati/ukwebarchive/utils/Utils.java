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
 *
 * @author pierpaolo
 */
public class Utils {

    /**
     *
     * @param file
     * @param filterComment
     * @return
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
     *
     * @param dateString
     * @return
     */
    public static String getYearMonthFromDateStr(String dateString) {
        return dateString.substring(0, 6);
    }

    /*public static void printStatistics(Map<String, Map<String, Long>> statistics) {
        System.out.println();
        Set<String> dateKeySet = statistics.keySet();
        for (String dateKey : dateKeySet) {
            Map<String, Long> tmap = statistics.get(dateKey);
            Set<String> typeKeySet = tmap.keySet();
            for (String typeKey : typeKeySet) {
                System.out.println(dateKey + "\t" + typeKey + "\t" + tmap.get(typeKey));
            }
        }
    }

    public static void saveStatistics(Map<String, Map<String, Long>> statistics, File file) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        Set<String> dateKeySet = statistics.keySet();
        for (String dateKey : dateKeySet) {
            Map<String, Long> tmap = statistics.get(dateKey);
            Set<String> typeKeySet = tmap.keySet();
            for (String typeKey : typeKeySet) {
                writer.append(dateKey + "\t" + typeKey + "\t" + tmap.get(typeKey));
                writer.newLine();
            }
        }
        writer.close();
    }*/
    /**
     *
     * @param statistics
     * @param file
     * @throws IOException
     */
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
    public static void printStatistics(Map<String, Long> statistics) {
        System.out.println();
        List<String> list = new ArrayList<>(statistics.keySet());
        Collections.sort(list);
        for (String key : list) {
            System.out.println(key + "\t" + statistics.get(key).toString());
        }
    }

    /**
     *
     * @param analyzer
     * @param fieldname
     * @param reader
     * @return
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
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static String getContent(File file) throws IOException {
        Document doc = Jsoup.parse(file, null);
        return doc.body().text();
    }

    /**
     *
     * @param stream
     * @return
     * @throws IOException
     */
    public static String getContent(InputStream stream) throws IOException {
        Document doc = Jsoup.parse(stream, null, "");
        return doc.body().text();
    }

    /**
     *
     * @param contentType
     * @return
     */
    public static String getBaseContentType(String contentType) {
        return contentType.split(";")[0].trim();
    }

}
