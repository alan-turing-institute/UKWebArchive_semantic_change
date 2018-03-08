/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package di.uniba.it.ukwebarchive.idx;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author pierpaolo
 */
public class CdxReader implements Iterator<CdxData> {

    private final BufferedReader reader;

    private boolean close = false;

    private Map<String, Integer> fields = null;

    /**
     *
     * @param filename
     * @throws IOException
     */
    public CdxReader(String filename) throws IOException {
        this(new File(filename));
    }

    /**
     *
     * @param file
     * @throws IOException
     */
    public CdxReader(File file) throws IOException {
        if (file.getName().endsWith(".gz")) {
            reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
        } else {
            reader = new BufferedReader(new FileReader(file));
        }
    }

    private void buildHeader(String line) {
        fields = new HashMap<>();
        String[] split = line.trim().split(" ");
        for (int i = 1; i < split.length; i++) {
            fields.put(split[i], i - 1);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (close) {
                return false;
            }
            boolean ready = reader.ready();
            if (ready) {
                return true;
            } else {
                reader.close();
                close = true;
                return false;
            }
        } catch (IOException ex) {
            Logger.getLogger(CdxReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    @Override
    public CdxData next() {
        try {
            String line = reader.readLine();
            if (line.trim().startsWith("CDX")) {
                buildHeader(line);
                if (hasNext()) {
                    return next();
                } else {
                    throw new IOException("CDX file is empty");
                }
            } else {
                String[] split = line.split(" ");
                if (fields != null) {
                    CdxData idxData = new CdxData();
                    idxData.setName(split[fields.get("N")]);
                    idxData.setDateString(split[fields.get("b")]);
                    idxData.setUrl(split[fields.get("a")]);
                    idxData.setMineType(split[fields.get("m")]);
                    idxData.setHttpResponse(split[fields.get("s")]);
                    idxData.setOffset(Long.parseLong(split[fields.get("V")]));
                    idxData.setArcfile(split[fields.get("g")]);
                    return idxData;
                } else {
                    CdxData idxData = new CdxData();
                    idxData.setName(split[0]);
                    idxData.setDateString(split[1]);
                    idxData.setUrl(split[2]);
                    idxData.setMineType(split[3]);
                    idxData.setHttpResponse(split[4]);
                    if (split[7].matches("[0-9]+")) {
                        idxData.setOffset(Long.parseLong(split[7]));
                    } else {
                        idxData.setOffset(-1);
                    }
                    idxData.setArcfile(split[8]);
                    return idxData;
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(CdxReader.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            CdxReader reader = new CdxReader("/home/pierpaolo/dataset/ukwebarchive/DOTUK-HISTORICAL-1996-2010-GROUP-AA-XAAAAA-20110428000000-00000.arc.os.cdx.gz");
            //CdxReader reader = new CdxReader("/home/pierpaolo/dataset/ukwebarchive/DOTUK-HISTORICAL-1996-2010-PHASE2WARCS-XAAAAA-20111115000000-000000.warc.os.cdx.gz");
            int c = 0;
            while (reader.hasNext()) {
                CdxData next = reader.next();
                System.out.println(next.getHttpResponse() + "\t" + next.getMineType() + "\t" + next.getDateString());
                c++;
            }
            System.out.println(c);
        } catch (IOException ex) {
            Logger.getLogger(CdxReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
