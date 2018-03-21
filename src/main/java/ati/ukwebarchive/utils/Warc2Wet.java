/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.utils;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

/**
 * This is a utility class that converts an Warc file to a Wet file
 * 
 * @author pierpaolo
 */
public class Warc2Wet {

    /**
     * @param args the command line arguments: the first argument is the warc file the secondo is one is the wet file
     */
    public static void main(String[] args) {
        try {
            WarcReader warcReader = WarcReaderFactory.getReader(new FileInputStream(args[0]));
            WarcWriter warcWriter = WarcWriterFactory.getWriterCompressed(new FileOutputStream(args[1]), 8196);
            int records = 0;
            int errors = 0;
            int notValidHeader = 0;
            int notValidPayload = 0;
            int ok = 0;
            int zeroL = 0;
            WarcRecord record;
            while ((record = warcReader.getNextRecord()) != null) {
                try {
                    HttpHeader httpHeader = record.getHttpHeader();
                    if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null) {
                        if (httpHeader.statusCodeStr.equals("200") && httpHeader.contentType.startsWith("text/")) {
                            WarcRecord wetRecord = WarcRecord.createRecord(warcWriter);
                            InputStream is = httpHeader.getPayloadInputStream();
                            if (is != null && is.available() > 0) {
                                String text = Utils.getContent(is);
                                text = text.replaceAll("\\n+", "\n");
                                if (text.length() > 0) {
                                    byte[] bytes = text.getBytes();
                                    wetRecord.header.warcDate = record.header.warcDate;
                                    wetRecord.header.warcDateStr = record.header.warcDateStr;
                                    wetRecord.header.warcTargetUriStr = record.header.warcTargetUriStr;
                                    wetRecord.header.warcTargetUriUri = record.header.warcTargetUriUri;
                                    wetRecord.header.contentLength = new Long(bytes.length);
                                    wetRecord.header.contentLengthStr = String.valueOf(bytes.length);
                                    wetRecord.header.contentType = record.header.contentType;
                                    wetRecord.header.contentTypeStr = record.header.contentTypeStr;
                                    InputStream wis = new ByteArrayInputStream(bytes);
                                    warcWriter.writeHeader(wetRecord);
                                    warcWriter.streamPayload(wis);
                                    ok++;
                                } else {
                                    zeroL++;
                                }
                            } else {
                                notValidPayload++;
                            }
                        } else {
                            notValidHeader++;
                        }
                    } else {
                        notValidHeader++;
                    }
                    record.close();
                    records++;
                } catch (Exception ex) {
                    Logger.getLogger(Warc2Wet.class.getName()).log(Level.WARNING, "Error in wat record", ex);
                    errors++;
                }
            }
            warcReader.close();
            warcWriter.close();
            System.out.println(records + "\t" + errors + "\t" + notValidHeader + "\t" + notValidPayload + "\t" + zeroL + "\t" + ok);
        } catch (Exception ex) {
            Logger.getLogger(Warc2Wet.class.getName()).log(Level.SEVERE, "Main error", ex);
        }
    }

}
