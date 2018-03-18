/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author pierpaolo
 */
public class FixCorpusGZ {

    private static final Logger LOG = Logger.getLogger(FixCorpusGZ.class.getName());

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length > 1) {
            BufferedWriter writer = null;
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(args[0]))));
                writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(args[1]))));
                StringBuilder sb = new StringBuilder();
                String lastDocHeader = null;
                while (reader.ready()) {
                    String line = reader.readLine();
                    if (line.startsWith("#--DOC START--")) {
                        if (sb.length() > 0) {
                            writer.append(lastDocHeader);
                            writer.newLine();
                            writer.append(formatDoc(sb.toString()));
                            writer.newLine();
                            sb = new StringBuilder();
                        }
                        lastDocHeader = line;
                    } else {
                        sb.append(line);
                        sb.append("\n");
                    }
                }
                if (sb.length() > 0) {
                    writer.append(formatDoc(sb.toString()));
                    writer.newLine();
                    sb = new StringBuilder();
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

    private static String formatDoc(String doc) {
        doc = doc.replaceAll("[ \\t\\x0B\\f\\r\\n]+", " ");
        //doc = doc.replaceAll("[\\r\\n]+", "\n");
        return doc.trim();
    }

}
