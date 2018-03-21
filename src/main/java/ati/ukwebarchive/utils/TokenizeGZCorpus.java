/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 *
 * @author pierpaolo
 */
@Deprecated
public class TokenizeGZCorpus {

    private static final Logger LOG = Logger.getLogger(TokenizeGZCorpus.class.getName());

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length > 1) {
            File mainDir = new File(args[0]);
            File[] files = mainDir.listFiles();
            for (File infile : files) {
                if (infile.isFile() && infile.getName().endsWith(".gz")) {
                    BufferedWriter writer = null;
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(infile))));
                        writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(args[1] + "/" + infile.getName()))));
                        StringBuilder sb = new StringBuilder();
                        while (reader.ready()) {
                            String line = reader.readLine();
                            if (line.startsWith("#--DOC START--")) {
                                if (sb.length() > 0) {
                                    List<String> tokens = Utils.getTokens(new StandardAnalyzer(), "text", new StringReader(sb.toString()));
                                    for (String t : tokens) {
                                        writer.append(t).append(" ");
                                    }
                                    writer.newLine();
                                    sb = new StringBuilder();
                                }
                            } else {
                                sb.append(line);
                                sb.append("\n");
                            }
                        }
                        if (sb.length() > 0) {
                            List<String> tokens = Utils.getTokens(new StandardAnalyzer(), "text", new StringReader(sb.toString()));
                            for (String t : tokens) {
                                writer.append(t).append(" ");
                            }
                            writer.newLine();
                        }
                    } catch (Exception ex) {
                        LOG.log(Level.SEVERE, null, ex);
                    } finally {
                        if (writer != null) {
                            try {
                                writer.close();
                            } catch (IOException ex) {
                                LOG.log(Level.SEVERE, null, ex);
                            }
                        }
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (IOException ex) {
                                LOG.log(Level.SEVERE, null, ex);
                            }
                        }
                    }
                }
            }
        }
    }

}
