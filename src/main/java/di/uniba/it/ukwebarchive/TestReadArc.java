/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package di.uniba.it.ukwebarchive;

import di.uniba.it.ukwebarchive.utils.Utils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;

/**
 * Dummy test class for reading ARC files
 * @author pierpaolo
 */
public class TestReadArc {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        File file = new File("/home/pierpaolo/dataset/ukwebarchive/DOTUK-HISTORICAL-1996-2010-GROUP-AA-XAAAAA-20110428000000-00000.arc.gz");
        
        int records = 0;
        int errors = 0;
        
        ArcReader reader = ArcReaderFactory.getReaderCompressed(new FileInputStream(file));
        ArcRecordBase record;
        
        while ((record = reader.getNextRecord()) != null) {
            if (Utils.getBaseContentType(record.getContentTypeStr()).startsWith("text/")) {
                printRecord(record);
            }
            
            ++records;
        }
        System.out.println("Records: " + records);
    }
    
    /**
     *
     * @param record
     * @throws IOException
     */
    public static void printRecord(ArcRecordBase record) throws IOException {
        /*InputStream payloadContent = record.getPayloadContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(payloadContent));
        while (reader.ready()) {
            System.out.println(reader.readLine());
        }
        reader.close();*/
        System.out.println(record.getUrlStr());
    }
    
}