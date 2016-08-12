import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttException;

/**
 * Create a consumer that will handle these events.
 * In our case all we want to do is print the value out the console.
 */
class MessagePublishEventHandler implements com.lmax.disruptor.EventHandler<MessageEvent> {

    private static final Log log = LogFactory.getLog(MessagePublishEventHandler.class);
    private LocalMqttClient mqClient;
    private final long ordinal;
    private final long numberOfConsumers;

    /**
     * @param mqClient Client parameters like Broker URL, Consumer ID and Number of Consumers.
     * @param ordinal Contains Consumer Id. (This should be less than number of  Consumers.)
     * @param numberOfConsumers Number of consumers passes through the main method.
     */
    MessagePublishEventHandler(LocalMqttClient mqClient, final long ordinal,
                                      final long numberOfConsumers) {
        this.mqClient = mqClient;
        this.ordinal = ordinal;
        this.numberOfConsumers = numberOfConsumers;
    }
    /**
     * Use values that set in the Constructor to publishing to mqtt clients using the ordinal.
     * */
    public void onEvent(MessageEvent messageEvent, long sequence, boolean endOfBatch) throws Exception {

        if ((sequence % numberOfConsumers) == ordinal) {
            try {

                byte[] stringEvent = messageEvent.getMessage().getBytes();
                log.info("MessageEvent: " + new String(stringEvent));

                // Publishing to mqtt topic "simpleTopic"
                mqClient.publishMessage(messageEvent.getMessage().getBytes());

            } catch (MqttException e) {
                log.error("Error happen when message publish ", e);
            } finally {
                // Clears the instance of ring buffer after delivering message to a MQTT client.
                messageEvent.clear();
            }
        }
    }
}

