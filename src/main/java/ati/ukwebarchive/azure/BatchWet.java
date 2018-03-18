/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.DetailLevel;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.AllocationState;
import com.microsoft.azure.batch.protocol.models.ApplicationPackageReference;
import com.microsoft.azure.batch.protocol.models.AutoUserScope;
import com.microsoft.azure.batch.protocol.models.AutoUserSpecification;
import com.microsoft.azure.batch.protocol.models.BatchErrorDetail;
import com.microsoft.azure.batch.protocol.models.BatchErrorException;
import com.microsoft.azure.batch.protocol.models.CloudTask;
import com.microsoft.azure.batch.protocol.models.ElevationLevel;
import com.microsoft.azure.batch.protocol.models.ImageReference;
import com.microsoft.azure.batch.protocol.models.NodeAgentSku;
import com.microsoft.azure.batch.protocol.models.OSType;
import com.microsoft.azure.batch.protocol.models.PoolAddParameter;
import com.microsoft.azure.batch.protocol.models.PoolInformation;
import com.microsoft.azure.batch.protocol.models.StartTask;
import com.microsoft.azure.batch.protocol.models.TaskAddParameter;
import com.microsoft.azure.batch.protocol.models.TaskState;
import com.microsoft.azure.batch.protocol.models.UserIdentity;
import com.microsoft.azure.batch.protocol.models.VirtualMachineConfiguration;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pierpaolo
 */
public class BatchWet {
    
    private static final Logger LOG = Logger.getLogger(BatchWet.class.getName());
    
    private static final String poolId = "ukwac";
    
    private static final String jobId = "ukwac2wet1996-2010";
    
    private static void printBatchException(BatchErrorException err) {
        LOG.log(Level.SEVERE, "BatchError {0}", err.toString());
        if (err.body() != null) {
            LOG.log(Level.SEVERE, "BatchError code={0}, message={1}", new Object[]{err.body().code(), err.body().message().value()});
            if (err.body().values() != null) {
                for (BatchErrorDetail detail : err.body().values()) {
                    LOG.log(Level.SEVERE, "Detail {0}={1}", new Object[]{detail.key(), detail.value()});
                }
            }
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            props.load(new FileReader("config.properties"));
            LOG.info("Connecting...");
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(props.getProperty("azure.batch.uri"),
                    props.getProperty("azure.batch.account"),
                    props.getProperty("azure.batch.key"));
            BatchClient client = BatchClient.open(cred);
            LOG.info("Creating pool of VMs...");
            String osPublisher = "Canonical";
            String osOffer = "UbuntuServer";
            List<NodeAgentSku> skus = client.accountOperations().listNodeAgentSkus();
            String skuId = null;
            ImageReference imageRef = null;
            for (NodeAgentSku sku : skus) {
                if (sku.osType() == OSType.LINUX) {
                    for (ImageReference imgRef : sku.verifiedImageReferences()) {
                        if (imgRef.publisher().equalsIgnoreCase(osPublisher) && imgRef.offer().equalsIgnoreCase(osOffer)) {
                            imageRef = imgRef;
                            skuId = sku.id();
                            break;
                        }
                    }
                }
            }
            VirtualMachineConfiguration configuration = new VirtualMachineConfiguration();
            configuration.withNodeAgentSKUId(skuId).withImageReference(imageRef);
            List<ApplicationPackageReference> appList = new ArrayList<>();
            appList.add(new ApplicationPackageReference().withApplicationId("ukwac").withVersion("1.0"));
            StartTask instJava = new StartTask().withUserIdentity(new UserIdentity()
                    .withAutoUser(new AutoUserSpecification()
                            .withElevationLevel(ElevationLevel.ADMIN).withScope(AutoUserScope.POOL))).withCommandLine("/bin/bash -c 'apt-get -y install openjdk-8-jre'").withWaitForSuccess(Boolean.TRUE);
            client.poolOperations().createPool(new PoolAddParameter()
                    .withId(poolId)
                    .withApplicationPackageReferences(appList)
                    .withVirtualMachineConfiguration(configuration)
                    .withVmSize("STANDARD_D2")
                    .withStartTask(instJava)
                    .withTargetDedicatedNodes(10));
            LOG.info("Create job...");
            PoolInformation poolInfo = new PoolInformation();
            poolInfo.withPoolId(poolId);
            client.jobOperations().createJob(jobId, poolInfo);
            LOG.info("Waiting for pool...");
            while (client.poolOperations().getPool(poolId).allocationState() != AllocationState.STEADY) {
                Thread.sleep(10000);
            }
            LOG.info("Creating task...");
            final String uri = props.getProperty("uri");
            CloudBlobContainer mainContainer = new CloudBlobContainer(new URI(uri));
            Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(props.getProperty("mainContainer"));
            int n = 0;
            for (ListBlobItem item : listBlobs) {
                if (item instanceof CloudBlobDirectory) {
                    CloudBlobDirectory cdir = (CloudBlobDirectory) item;
                    Iterable<ListBlobItem> listBlobs1 = cdir.listBlobs();
                    for (ListBlobItem itemBlock : listBlobs1) {
                        if (itemBlock instanceof CloudBlockBlob) {
                            CloudBlockBlob block = (CloudBlockBlob) itemBlock;
                            if (block.getName().endsWith(".arc.gz") || block.getName().endsWith(".warc.gz")) {
                                TaskAddParameter taskClientPar = new TaskAddParameter();
                                taskClientPar.withId("client-" + n).withCommandLine("/bin/bash -c '${AZ_BATCH_APP_PACKAGE_ukwac}/run.sh ati.ukwebarchive.azure.ClientTask " + block.getName() + "'");
                                client.taskOperations().createTask(jobId, taskClientPar);
                                n++;
                                if (n % 100 == 0) {
                                    Thread.sleep(30000);
                                    if (n % 1000 == 0) {
                                        LOG.log(Level.INFO, "Added task {0}", n);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            LOG.info("Wait for task to complete...");
            long startTime = System.currentTimeMillis();
            long elapsedTime = 0L;
            Duration expiryTime = Duration.ofHours(48);
            boolean terminate = false;
            while (elapsedTime < expiryTime.toMillis() && !terminate) {
                List<CloudTask> taskCollection = client.taskOperations().listTasks(jobId, new DetailLevel.Builder().withSelectClause("id, state").build());
                boolean allComplete = true;
                for (CloudTask task : taskCollection) {
                    if (task.state() != TaskState.COMPLETED) {
                        allComplete = false;
                        break;
                    }
                }
                if (allComplete) {
                    terminate = true;
                }
                LOG.info("wait 30 seconds for tasks to complete...");
                // Check again after 10 seconds
                Thread.sleep(30 * 1000);
                elapsedTime = (new Date()).getTime() - startTime;
            }
            if (terminate) {
                LOG.info("All tasks are completed");
            } else {
                LOG.warning("TIMEOUT");
            }
            LOG.info("Cleaning...");
            client.jobOperations().deleteJob(jobId);
            client.poolOperations().deletePool(poolId);
        } catch (BatchErrorException err) {
            printBatchException(err);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InterruptedException | URISyntaxException | StorageException ex) {
            Logger.getLogger(BatchWet.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
