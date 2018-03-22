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
 * This class executes a batch on a pool of VMs on Azure.
 * A single job is created in the pool. Each blob directory in the main storage container is executed as a single task in the pool.
 * 
 * @author pierpaolo
 */
public class WetBatchMulti {

    private static final Logger LOG = Logger.getLogger(WetBatchMulti.class.getName());

    private static final String poolId = "ukwac";

    private static final String jobId = "ukwac2wet1996-2010";

    // Log for a batch error
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
            //Load properties
            Properties props = new Properties();
            props.load(new FileReader("config.properties"));
            //Connect to a batch account in Azure
            LOG.info("Connecting...");
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(props.getProperty("azure.batch.uri"),
                    props.getProperty("azure.batch.account"),
                    props.getProperty("azure.batch.key"));
            BatchClient client = BatchClient.open(cred);
            //Create a pool in the batch
            LOG.info("Creating pool of VMs...");
            //Find information about the Linux Ubuntu IMG for the VM
            String osPublisher = "Canonical";
            String osOffer = "UbuntuServer";
            List<NodeAgentSku> skus = client.accountOperations().listNodeAgentSkus();
            String skuId = null;
            ImageReference imageRef = null;
            for (NodeAgentSku sku : skus) {
                if (sku.osType() == OSType.LINUX && sku.id().equals("batch.node.ubuntu 16.04")) {
                    for (ImageReference imgRef : sku.verifiedImageReferences()) {
                        if (imgRef.publisher().equalsIgnoreCase(osPublisher) && imgRef.offer().equalsIgnoreCase(osOffer)) {
                            imageRef = imgRef;
                            skuId = sku.id();
                            break;
                        }
                    }
                }
            }
            //Config VM
            VirtualMachineConfiguration configuration = new VirtualMachineConfiguration();
            configuration.withNodeAgentSKUId(skuId).withImageReference(imageRef);
            //Create an application for the pool. You need to load in the batch an application named ukwac with version 1.0. See https://docs.microsoft.com/en-us/azure/batch/batch-application-packages
            //The application is a zip file containing the jar of this process all the resource files (properties and contentFilter) and the script run.sh
            List<ApplicationPackageReference> appList = new ArrayList<>();
            appList.add(new ApplicationPackageReference().withApplicationId("ukwac").withVersion("1.0"));
            //IMPORTANT create a start task for installing JAVA on VMs in the pool
            StartTask instJava = new StartTask().withUserIdentity(new UserIdentity()
                    .withAutoUser(new AutoUserSpecification()
                            .withElevationLevel(ElevationLevel.ADMIN).withScope(AutoUserScope.POOL))).withCommandLine("/bin/bash -c 'apt-get -y install openjdk-8-jre'").withWaitForSuccess(Boolean.TRUE);
            //create the pool of VMs
            client.poolOperations().createPool(new PoolAddParameter()
                    .withId(poolId)
                    .withApplicationPackageReferences(appList)
                    .withVirtualMachineConfiguration(configuration)
                    .withVmSize(props.getProperty("pool.vmSize"))
                    .withStartTask(instJava)
                    .withTargetDedicatedNodes(Integer.parseInt(props.getProperty("pool.nodes"))));
            //create the job
            LOG.info("Create job...");
            PoolInformation poolInfo = new PoolInformation();
            poolInfo.withPoolId(poolId);
            client.jobOperations().createJob(jobId, poolInfo);
            //waitining for the pool creation
            LOG.info("Waiting for pool...");
            while (client.poolOperations().getPool(poolId).allocationState() != AllocationState.STEADY) {
                Thread.sleep(10000);
            }
            //create tasks in the job
            LOG.info("Creating task...");
            final String uri = props.getProperty("uri");
            CloudBlobContainer mainContainer = new CloudBlobContainer(new URI(uri));
            Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(props.getProperty("mainContainer"));
            int n = 0;
            for (ListBlobItem item : listBlobs) {
                //for each directory in the main container create a new task
                if (item instanceof CloudBlobDirectory) {
                    CloudBlobDirectory cdir = (CloudBlobDirectory) item;
                    TaskAddParameter taskClientPar = new TaskAddParameter();
                    taskClientPar.withId("client-" + n).withCommandLine("/bin/bash -c '${AZ_BATCH_APP_PACKAGE_ukwac}/run.sh ati.ukwebarchive.azure.BlobDir2WetProcessor " + cdir.getPrefix() + "'");
                    client.taskOperations().createTask(jobId, taskClientPar);
                    n++;
                    LOG.log(Level.INFO, "Added task {0}", n);
                }
            }
            //wait for task to complete 
            LOG.info("Wait for task to complete...");
            long startTime = System.currentTimeMillis();
            long elapsedTime = 0L;
            //the max duration of a job on Azure is 7 days
            Duration expiryTime = Duration.ofDays(7);
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
                LOG.info("wait 1 minute for tasks to complete...");
                // Check again after 1 minute
                Thread.sleep(60 * 1000);
                elapsedTime = (new Date()).getTime() - startTime;
            }
            if (terminate) {
                LOG.info("All tasks are completed");
            } else {
                LOG.warning("TIMEOUT");
            }
            //Eventually remove job and pool
            //LOG.info("Cleaning...");
            //client.jobOperations().deleteJob(jobId);
            //client.poolOperations().deletePool(poolId);
        } catch (BatchErrorException err) {
            printBatchException(err);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InterruptedException | URISyntaxException | StorageException ex) {
            Logger.getLogger(WetBatchMulti.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
