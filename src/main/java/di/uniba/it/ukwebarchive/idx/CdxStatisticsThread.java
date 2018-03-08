/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package di.uniba.it.ukwebarchive.idx;

import di.uniba.it.ukwebarchive.data.CloudBlockMsg;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import di.uniba.it.ukwebarchive.utils.Utils;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pierpaolo
 */
public class CdxStatisticsThread extends Thread {

    private final ConcurrentLinkedQueue<CloudBlockMsg> queue;

    private boolean run = true;

    /**
     *
     * @param queue
     */
    public CdxStatisticsThread(ConcurrentLinkedQueue<CloudBlockMsg> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (run) {
            CloudBlockMsg msg = queue.poll();
            if (msg != null) {
                if (msg.isValid()) {
                    CloudBlockBlob block = msg.getBlock();
                    try {
                        int idxLastPathPart = block.getName().lastIndexOf("/");
                        if (idxLastPathPart > 0) {
                            String tmpFilename = block.getName().substring(idxLastPathPart + 1);
                            File tmpPath = new File(CdxStatisticsMT.props.getProperty("tempDir") + "/" + tmpFilename);
                            block.downloadToFile(tmpPath.getAbsolutePath());
                            long error = 0;
                            long ok = 0;
                            CdxReader cdxreader = new CdxReader(tmpPath);
                            Map<String, Long> cs = new HashMap<>();
                            while (cdxreader.hasNext()) {
                                try {
                                    CdxData cdxdata = cdxreader.next();
                                    if (cdxdata.getHttpResponse() != null && cdxdata.getDateString() != null && cdxdata.getMineType() != null) {
                                        if (cdxdata.getHttpResponse().equals("200") && CdxStatisticsMT.validTypeSet.contains(Utils.getBaseContentType(cdxdata.getMineType()))) {
                                            String key = Utils.getBaseContentType(cdxdata.getDateString());
                                            Long c = cs.get(key);
                                            if (c == null) {
                                                cs.put(key, 1L);
                                            } else {
                                                cs.put(key, c + 1);
                                            }
                                            ok++;
                                        }
                                    }
                                } catch (Exception ex) {
                                    error++;
                                }
                            }
                            for (Map.Entry<String, Long> e : cs.entrySet()) {
                                CdxStatisticsMT.updateStatistics(e.getKey(), e.getValue());
                            }
                            tmpPath.delete();
                            CdxStatisticsMT.updateBlockCounter(error, ok);
                        }
                    } catch (Exception ex) {
                        Logger.getLogger(CdxStatisticsThread.class.getName()).log(Level.WARNING, "Skip block: " + block.getName(), ex);
                    }
                } else {
                    run = false;
                }
            }
        }
    }

    @Override
    public void interrupt() {
        this.run = false;
        super.interrupt();
    }

}
