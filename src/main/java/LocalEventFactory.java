import com.lmax.disruptor.EventFactory;

/**
 * In order to allow the Disruptor to preallocate these events,
 * We need an EventFactory that will perform the construction.
 */
class LocalEventFactory implements EventFactory<MessageEvent> {

    /**
     * Creating new instance for fill the ring buffer.
     * @return Created instance.
     */
    public MessageEvent newInstance() {
        return new MessageEvent();
    }

}
