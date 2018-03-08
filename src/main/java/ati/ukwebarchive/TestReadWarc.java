/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderCompressed;
import org.jwat.warc.WarcRecord;

/**
 * Dummy test class for reading WARC file using CDX offsets
 * @author pierpaolo
 */
public class TestReadWarc {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {

        File cdxFile = new File("/home/pierpaolo/dataset/ukwebarchive/DOTUK-HISTORICAL-1996-2010-PHASE2WARCS-XAAAAA-20111115000000-000000.warc.os.cdx.gz");
        File warcFile = new File("/home/pierpaolo/dataset/ukwebarchive/DOTUK-HISTORICAL-1996-2010-PHASE2WARCS-XAAAAA-20111115000000-000000.warc.gz");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(cdxFile))));
        WarcReader warcReader = new WarcReaderCompressed();
        warcReader.setBlockDigestEnabled(true);
        warcReader.setPayloadDigestEnabled(true);
        Map<String, Integer> header = new HashMap<>();
        if (reader.ready()) {
            String[] split = reader.readLine().split(" ");
            for (int i = 2; i < split.length; i++) {
                header.put(split[i], i - 2);
            }
        }
        int records = 0;
        while (reader.ready()) {
            String line = reader.readLine();
            if (!line.startsWith("filedesc:")) {
                String[] split = line.split(" ");
                String response = split[header.get("s")];
                String offsetStr = split[header.get("V")];
                String mimeType = split[header.get("m")];
                if (response.equals("200") && mimeType.startsWith("text/") && offsetStr.matches("[0-9]+")) {
                    System.out.println(response + "\t" + offsetStr + "\t" + mimeType);
                    if (offsetStr.matches("[0-9]+")) {
                        FileInputStream warcStream = new FileInputStream(warcFile);
                        WarcRecord warcRecord = warcReader.getNextRecordFrom(warcStream, Long.parseLong(offsetStr), 1024 * 32);
                        InputStream is = warcRecord.getHttpHeader().getPayloadInputStream();
                        BufferedReader bf = new BufferedReader(new InputStreamReader(is));
                        while (bf.ready()) {
                            System.out.println(bf.readLine());
                        }
                        bf.close();
                        records++;
                    }
                }
            }
        }
        reader.close();
        warcReader.close();
        System.out.println("Records: " + records);
    }

}
