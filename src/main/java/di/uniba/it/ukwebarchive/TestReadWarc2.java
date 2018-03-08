/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package di.uniba.it.ukwebarchive;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

/**
 * Dummy test class for reading WARC files
 * @author pierpaolo
 */
public class TestReadWarc2 {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {

        File warcFile = new File("/home/pierpaolo/dataset/ukwebarchive/DOTUK-HISTORICAL-1996-2010-PHASE2WARCS-XAAAAA-20111115000000-000000.warc.gz");
        WarcReader warcReader = WarcReaderFactory.getReader(new FileInputStream(warcFile));
        int records = 0;
        WarcRecord record;
        while ((record = warcReader.getNextRecord()) != null) {
            HttpHeader httpHeader = record.getHttpHeader();
            if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null) {
                if (httpHeader.statusCodeStr.equals("200") && httpHeader.contentType.startsWith("text/")) {
                    System.out.println(record.header.warcDateStr);
                    System.out.println(httpHeader.contentType);
                    System.out.println(record.header.warcTargetUriStr);
                    /*InputStream is = httpHeader.getPayloadInputStream();
                    BufferedReader reader=new BufferedReader(new InputStreamReader(is));
                    while (reader.ready()) {
                        System.out.println(reader.readLine());
                    }
                    reader.close();*/
                    records++;
                }
            }
        }
        warcReader.close();
        System.out.println("Records: " + records);
    }

}
