import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;

/**
 * Handles Input messages from a text file , then put them into a disruptor and
 * finally put messages in disruptor in to a selected MB by using MQTT client.
 */

public class EventMain {

    private static final Log log = LogFactory.getLog(EventMain.class);

    public static void main(String[] args)  {

        // Executor that will be used to construct new threads for consumers.
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("MQTTPublisherThreadPool-%d").build();

        // The factory for the event consists of new instances.
        LocalEventFactory factory = new LocalEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 8;

        // Construct the Disruptor
        Disruptor<MessageEvent> disruptor = new Disruptor<MessageEvent>(
                factory, bufferSize, threadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());

        int numberOfConsumer = 2;

        LocalMqttClient[] localMqttClient = new LocalMqttClient[numberOfConsumer];
        MessagePublishEventHandler[] messagePublishEventHandler = new MessagePublishEventHandler[numberOfConsumer];

        EventMain.disruptorHandler(localMqttClient, messagePublishEventHandler, numberOfConsumer);

        disruptor.handleEventsWith(messagePublishEventHandler);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<MessageEvent> ringBuffer = disruptor.getRingBuffer();

        EventProducer producer = new EventProducer(ringBuffer);
        EventMain.fileReader(producer);
        disruptor.halt();
        disruptor.shutdown();
        log.info("Disruptor shutdown");
    }

    /**
     * Read the given file and put each line as a message and put it in to the disruptor.
     * @param producer Make an instance to Put given messages to the Ring Buffer.
     */
    private static void fileReader(EventProducer producer) {

        //Set File path
        String fileName = "temp.txt";
        String line;
        int lines = 0;

        // variable lines Calculate number of lines of Selected file.
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(fileName));
            while (reader.readLine() != null) {
                lines++;
            }
        } catch (FileNotFoundException e) {
            log.error("Couldn't find the text file", e);
        } catch (IOException e) {
            log.error("Failed or interrupted I/O operations while reading the file", e);
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                log.error("Error occur when file Reader closed ", e);
            }
        }

        FileReader fileReader = null;
        BufferedReader bufferedReader = null;

        try {
            //count number of lines on  the  temp.txt file.

            // FileReader reads text files in the default encoding..
            fileReader = new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            bufferedReader = new BufferedReader(fileReader);

            for (int i = 0; i < lines; i++) {
                //read line by line.
                line = bufferedReader.readLine();
                // submit messages to write concurrently using disruptor
                producer.onData(line);
            }
        } catch (FileNotFoundException e) {
            log.error("File with the specified pathname does not exist", e);
        } catch (IOException e) {
            log.error("Failed or interrupted I/O operations", e);
        } finally {
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e) {
                    log.error("Error occur when fileReader closed ", e);
                }
            }

            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    log.error("Error occur when bufferedReader closed ", e);
                }
            }
        }
    }

    /**
     * Handle the output of the disruptor and put messages in to the selected MB.
     * @param localMqttClient Make MQTT client to send/receive messages.
     * @param messagePublishEventHandler Consumer that will handle the event to print the value.
     * @param numberOfConsumers Number of clients that pass messages to the message broker.
     */
    private static void disruptorHandler(
            LocalMqttClient[] localMqttClient, MessagePublishEventHandler[] messagePublishEventHandler,
            int numberOfConsumers) {

        for (int r = 0; r < numberOfConsumers; r++) {
            int mods = r % 2; // Because we use only 2 MB's
            int port = 1883 + mods; // Making MB URL port
            // Connect the handler
            localMqttClient[r] = new LocalMqttClient("tcp://localhost:" + port, "Topic " + r, "publisher " + r);
            messagePublishEventHandler[r] = new MessagePublishEventHandler(localMqttClient[r], r, numberOfConsumers);
        }
    }
}

