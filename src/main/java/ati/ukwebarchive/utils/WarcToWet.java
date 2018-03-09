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
import org.apache.tika.exception.TikaException;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

/**
 *
 * @author pierpaolo
 */
public class WarcToWet {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws TikaException {
        try {
            Tika tika = new Tika();
            WarcReader warcReader = WarcReaderFactory.getReader(new FileInputStream(args[0]), 8196);
            WarcWriter warcWriter = WarcWriterFactory.getWriterCompressed(new FileOutputStream(args[1]), 8196);
            int records = 0;
            WarcRecord record;
            while ((record = warcReader.getNextRecord()) != null) {
                try {
                    HttpHeader httpHeader = record.getHttpHeader();
                    if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null) {
                        if (httpHeader.statusCodeStr.equals("200") && httpHeader.contentType.startsWith("text/")) {
                            WarcRecord wetRecord = WarcRecord.createRecord(warcWriter);
                            InputStream is = httpHeader.getPayloadInputStream();
                            if (is.available() > 0) {
                                String text = tika.parseToString(is);
                                text = text.replaceAll("\\n+", "\n");
                                byte[] bytes = text.getBytes();
                                wetRecord.header.contentLength = new Long(bytes.length);
                                wetRecord.header.contentLengthStr = String.valueOf(bytes.length);
                                InputStream wis = new ByteArrayInputStream(bytes);
                                warcWriter.writeHeader(wetRecord);
                                warcWriter.streamPayload(wis);
                                records++;
                            }
                        }
                    }
                } catch (Exception ex) {
                    Logger.getLogger(WarcToWet.class.getName()).log(Level.WARNING, "Error in wat record", ex);
                }
            }
            warcReader.close();
            warcWriter.close();
            System.out.println(records);
        } catch (Exception ex) {
            Logger.getLogger(WarcToWet.class.getName()).log(Level.SEVERE, "Main error", ex);
        }
    }

}