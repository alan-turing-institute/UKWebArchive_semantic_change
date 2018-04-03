/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

/**
 * Dummy test class for reading WET files
 *
 * @author pierpaolo
 */
public class TestReadWet {

    /**
     * @param args the command line arguments args[0] is the WET filename
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {

        File warcFile = new File(args[0]);
        WarcReader warcReader = WarcReaderFactory.getReader(new FileInputStream(warcFile));
        int records = 0;
        WarcRecord record;
        while ((record = warcReader.getNextRecord()) != null) {

            System.out.println(record.header.warcDateStr);
            System.out.println(record.header.warcTargetUriStr);
            System.out.println(record.header.contentTypeStr);
            System.out.println(record.header.contentLength);
            /*InputStream is = record.getPayloadContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            while (reader.ready()) {
                System.out.println(reader.readLine());
            }
            reader.close();*/
            records++;

        }
        warcReader.close();
        System.out.println("Records: " + records);
    }

}
