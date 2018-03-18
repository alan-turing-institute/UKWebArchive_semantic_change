package ati.ukwebarchive.busservice;

import com.microsoft.azure.servicebus.ExceptionPhase;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageHandler;
import com.microsoft.azure.servicebus.IQueueClient;
import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.MessageHandlerOptions;
import com.microsoft.azure.servicebus.QueueClient;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BasicSendReceiveWithQueueClient {

    private static final Logger LOG = Logger.getLogger(BasicSendReceiveWithQueueClient.class.getName());

    // Connection String for the namespace can be obtained from the Azure portal under the
    // 'Shared Access policies' section.
    private static final String connectionString = "Endpoint=sb://diachronicukwac.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Oh/2nTlhPkpaSfsIYEn1HskpxVhlObauzQEhECK9VJU=";
    private static final String queueName = "queuetest";
    private static IQueueClient queueClient;
    private static int totalSend = 100;
    private static int totalReceived = 0;

    public static void main(String[] args) throws Exception {

        LOG.log(Level.INFO, "Starting BasicSendReceiveWithQueueClient sample");

        // create client
        LOG.log(Level.INFO, "Create queue client.");
        queueClient = new QueueClient(new ConnectionStringBuilder(connectionString, queueName), ReceiveMode.PEEKLOCK);

        // send and receive
        queueClient.registerMessageHandler(new MessageHandler(queueClient), new MessageHandlerOptions(1, false, Duration.ofMinutes(1)));
        for (int i = 0; i < totalSend; i++) {
            int j = i;
            LOG.log(Level.INFO, "Sending message {0}.", j);
            queueClient.sendAsync(new Message("" + i)).thenRunAsync(() -> {
                LOG.log(Level.INFO, "Sent message {0}.", j);
            });
        }

        while (totalReceived != totalSend) {
            Thread.sleep(1000);
        }

        LOG.log(Level.INFO, "Received all messages, exiting the sample.");
        LOG.log(Level.INFO, "Closing queue client.");
        queueClient.close();
    }

    static class MessageHandler implements IMessageHandler {

        private IQueueClient client;

        public MessageHandler(IQueueClient client) {
            this.client = client;
        }

        @Override
        public CompletableFuture<Void> onMessageAsync(IMessage iMessage) {
            LOG.log(Level.INFO, "Received message with sq#: {0} and lock token: {1}.", new Object[]{iMessage.getSequenceNumber(), iMessage.getLockToken()});
            return this.client.completeAsync(iMessage.getLockToken()).thenRunAsync(() -> {
                LOG.log(Level.INFO, "Completed message sq#: {0} and locktoken: {1}", new Object[]{iMessage.getSequenceNumber(), iMessage.getLockToken()});
                totalReceived++;
            });
        }

        @Override
        public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
            LOG.log(Level.INFO, "{0}-{1}", new Object[]{exceptionPhase, throwable.getMessage()});
        }
    }
}
