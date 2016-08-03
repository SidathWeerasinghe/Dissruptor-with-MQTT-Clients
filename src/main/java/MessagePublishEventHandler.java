import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Create a consumer that will handle these events.
 * In our case all we want to do is print the value out the the console.
 */
class MessagePublishEventHandler implements com.lmax.disruptor.EventHandler<Event> {

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
     * Use values that set in the Constructor.
     * */
    public void onEvent(Event event, long sequence, boolean endOfBatch) throws Exception {

        if ((sequence % numberOfConsumers) == ordinal) {
            try {
                byte[] stringEvent = event.getValue().getBytes();
                log.info("Event: " + new String(stringEvent));

                // Publishing to mqtt topic "simpleTopic"
                mqClient.publishMessage(event.getValue().getBytes());
            } catch (Exception e) {
                log.info("Error happen when message publish ", e);
            } finally {
                //use to clearValue all the events
                event.clearValue();
            }
        }
    }
}

