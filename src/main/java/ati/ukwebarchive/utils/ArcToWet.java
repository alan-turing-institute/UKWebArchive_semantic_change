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
import org.apache.tika.Tika;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

/**
 *
 * @author pierpaolo
 */
public class ArcToWet {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Tika tika = new Tika();
            ArcReader arcReader = ArcReaderFactory.getReader(new FileInputStream(args[0]));
            WarcWriter warcWriter = WarcWriterFactory.getWriterCompressed(new FileOutputStream(args[1]), 8196);
            int records = 0;
            int errors = 0;
            int notValidHeader = 0;
            int notValidPayload = 0;
            int ok = 0;
            int zeroL = 0;
            ArcRecordBase record;
            while ((record = arcReader.getNextRecord()) != null) {
                try {
                    HttpHeader httpHeader = record.getHttpHeader();
                    if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null) {
                        if (httpHeader.statusCodeStr.equals("200") && httpHeader.contentType.startsWith("text/")) {
                            WarcRecord wetRecord = WarcRecord.createRecord(warcWriter);
                            InputStream is = httpHeader.getPayloadInputStream();
                            if (is != null && is.available() > 0) {
                                String text = tika.parseToString(is);
                                text = text.replaceAll("\\n+", "\n");
                                if (text.length() > 0) {
                                    byte[] bytes = text.getBytes();
                                    wetRecord.header.warcDate = record.header.archiveDate;
                                    wetRecord.header.warcDateStr = record.header.archiveDateStr;
                                    wetRecord.header.warcTargetUriStr = record.header.urlStr;
                                    wetRecord.header.warcTargetUriUri = record.header.urlUri;
                                    wetRecord.header.contentLength = new Long(bytes.length);
                                    wetRecord.header.contentLengthStr = String.valueOf(bytes.length);
                                    wetRecord.header.contentType=record.header.contentType;
                                    wetRecord.header.contentTypeStr=record.header.contentTypeStr;
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
                    Logger.getLogger(ArcToWet.class.getName()).log(Level.WARNING, "Error in wat record", ex);
                    errors++;
                }
            }
            arcReader.close();
            warcWriter.close();
            System.out.println(records + "\t" + errors + "\t" + notValidHeader + "\t" + notValidPayload + "\t" + zeroL + "\t" + ok);
        } catch (Exception ex) {
            Logger.getLogger(ArcToWet.class.getName()).log(Level.SEVERE, "Main error", ex);
        }
    }

}
