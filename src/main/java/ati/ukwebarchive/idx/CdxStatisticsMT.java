/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.idx;

import ati.ukwebarchive.content.ContentThread;
import ati.ukwebarchive.data.CloudBlockMsg;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import ati.ukwebarchive.utils.Utils;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pierpaolo
 */
public class CdxStatisticsMT {

    /**
     *
     */
    public static Properties props;

    /**
     *
     */
    public static Set<String> validTypeSet;

    /**
     *
     */
    public static Map<String, Long> statistics;

    private static int blockCount = 0;

    private static long parsingError = 0;

    private static long parsingOk = 0;

    /**
     *
     */
    public synchronized static void updateBlockCounter() {
        blockCount++;
        if (blockCount % 10 == 0) {
            System.out.print(".");
            if (blockCount % 1000 == 0) {
                System.out.println(blockCount);
                Utils.printStatistics(statistics);
            }
        }
    }

    /**
     *
     * @param datekey
     * @param counter
     */
    public synchronized static void updateStatistics(String datekey, long counter) {
        Long c = statistics.get(datekey);
        if (c == null) {
            statistics.put(datekey, 1L);
        } else {
            statistics.put(datekey, c + counter);
        }
    }

    /**
     *
     * @param error
     * @param ok
     */
    public synchronized static void updateBlockCounter(long error, long ok) {
        blockCount++;
        parsingError += error;
        parsingOk += ok;
        Logger.getLogger(CdxStatisticsMT.class.getName()).log(Level.INFO, "PR:{0}\t{1}\t{2}", new Object[]{blockCount, parsingOk, parsingError});
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            props = new Properties();
            props.load(new FileReader("config.properties"));
            validTypeSet = Utils.loadFileInSet(new File(props.getProperty("contentTypeFilterFile")), true);
            statistics=new HashMap<>();
            File tmpDir = new File(props.getProperty("tempDir"));
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }
            final String uri = props.getProperty("uri");
            CloudBlobContainer mainContainer = new CloudBlobContainer(new URI(uri));
            Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(props.getProperty("mainContainer"));
            int nt = Integer.parseInt(props.getProperty("mt.n"));
            ConcurrentLinkedQueue<CloudBlockMsg> queue = new ConcurrentLinkedQueue<>();
            int thfull = nt * 250;
            List<ContentThread> threads = new ArrayList<>();
            for (int i = 0; i < nt; i++) {
                threads.add(new ContentThread(queue));
                threads.get(i).start();
            }
            for (ListBlobItem item : listBlobs) {
                if (item instanceof CloudBlobDirectory) {
                    CloudBlobDirectory cdir = (CloudBlobDirectory) item;
                    Iterable<ListBlobItem> listBlobs1 = cdir.listBlobs();
                    for (ListBlobItem itemBlock : listBlobs1) {
                        if (itemBlock instanceof CloudBlockBlob) {
                            CloudBlockBlob block = (CloudBlockBlob) itemBlock;
                            if (block.getName().endsWith(".cdx.gz")) {
                                if (queue.size() < thfull) {
                                    queue.offer(new CloudBlockMsg(block, true));
                                } else {
                                    try {
                                        while (queue.size() >= thfull) {
                                            Thread.sleep(10000);
                                        }
                                        queue.offer(new CloudBlockMsg(block, true));
                                    } catch (InterruptedException ex) {
                                        Logger.getLogger(CdxStatisticsMT.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for (int i = 0; i < nt; i++) {
                queue.add(new CloudBlockMsg(null, false));
            }
            for (int i = 0; i < nt; i++) {
                try {
                    threads.get(i).join();
                } catch (InterruptedException ex) {
                    Logger.getLogger(CdxStatisticsMT.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            Logger.getLogger(CdxStatisticsMT.class.getName()).log(Level.INFO, "END:{0}\t{1}\t{2}", new Object[]{blockCount, parsingOk, parsingError});
            Utils.saveStatistics(statistics, new File(props.getProperty("statistics.outFile")));
        } catch (IOException | URISyntaxException | StorageException ex) {
            Logger.getLogger(CdxStatisticsMT.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
