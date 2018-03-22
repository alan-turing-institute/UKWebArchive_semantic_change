/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure.tokenize;

import ati.ukwebarchive.data.CloudBlockMsg;
import ati.ukwebarchive.utils.Utils;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

/**
 * A thread that processes blob blocks from a queue and tokenize them.
 *
 * @author pierpaolo
 */
public class Blob2TokenThread extends Thread {

    private final ConcurrentLinkedQueue<CloudBlockMsg> queue;

    private final Properties props;

    private boolean run = true;

    private static final Logger LOG = Logger.getLogger(Blob2TokenThread.class.getName());

    public Blob2TokenThread(ConcurrentLinkedQueue<CloudBlockMsg> queue, Properties props) {
        this.queue = queue;
        this.props = props;
    }

    @Override
    public void run() {
        while (run) {
            CloudBlockMsg obj = queue.poll();
            if (obj != null) {
                if (obj.isValid()) {
                    CloudBlockBlob block = obj.getBlock();
                    String blockname = block.getName();
                    File tmpPath = null;
                    if (blockname.endsWith(".wet.gz")) {
                        LOG.log(Level.INFO, "Process block {0}.", blockname);
                        int ok = 0;
                        int error = 0;
                        try {
                            int idxLastPathPart = block.getName().lastIndexOf("/");
                            if (idxLastPathPart > 0) {
                                String tmpFilename = block.getName().substring(idxLastPathPart + 1);
                                tmpPath = new File(props.getProperty("tempDir") + "/" + tmpFilename);
                                block.downloadToFile(tmpPath.getAbsolutePath());
                                WarcReader warcReader = WarcReaderFactory.getReader(new FileInputStream(tmpPath));
                                WarcRecord record;
                                while ((record = warcReader.getNextRecord()) != null) {
                                    try {
                                        String datestr = record.header.warcDateStr;
                                        datestr = Utils.getYearMonthFromDateStr(datestr.replace("-", ""));
                                        InputStream is = record.getPayloadContent();
                                        List<String> tokens = Utils.getTokens(new StandardAnalyzer(), "text", new InputStreamReader(is));
                                        BlobDir2TokenProcessor.write(datestr, tokens);
                                        ok++;
                                    } catch (Exception | Error ex) {
                                        error++;
                                    }
                                }
                                warcReader.close();
                            }
                        } catch (Exception ex) {
                            LOG.log(Level.WARNING, "Skip block: " + blockname, ex);
                            try {
                                BlobDir2TokenProcessor.logMessage("Skip block: " + blockname + "\t" + ex.getMessage());
                            } catch (IOException ex1) {
                                Logger.getLogger(Blob2TokenThread.class.getName()).log(Level.SEVERE, null, ex1);
                            }
                        } finally {
                            if (tmpPath != null) {
                                tmpPath.delete();
                            }
                        }
                        LOG.log(Level.INFO, "Block {0}, ok {1}, error {2}", new Object[]{blockname, ok, error});
                        try {
                            BlobDir2TokenProcessor.logMessage("Processed block " + blockname + "\t" + ok + "\t" + error);
                        } catch (IOException ex) {
                            Logger.getLogger(Blob2TokenThread.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        System.gc();
                    }
                } else {
                    run = false;
                }
            } else {
                try {
                    this.sleep(3 * 1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Blob2TokenThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    @Override
    public void interrupt() {
        this.run = false;
    }

}
