import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Create a consumer that will handle these events.
 * In our case all we want to do is print the value out the the console.
 */
public class MessagePublishEventHandler implements com.lmax.disruptor.EventHandler<Event> {

    private static final Log log = LogFactory.getLog(MessagePublishEventHandler.class);
    LocalMqttClient mqClient;
    private final long ordinal;
    private final long numberOfConsumers;

    public MessagePublishEventHandler(LocalMqttClient mqClient, final long ordinal,
                                      final long numberOfConsumers) {
        this.mqClient = mqClient;
        this.ordinal = ordinal;
        this.numberOfConsumers = numberOfConsumers;
    }


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

