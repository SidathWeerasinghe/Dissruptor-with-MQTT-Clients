import com.lmax.disruptor.EventFactory;

/**
 * In order to allow the Disruptor to preallocate these events,
 * We need an EventFactory that will perform the construction.
 */
public class LocalEventFactory implements EventFactory<Event> {

    //Creating new instance for fill the ring buffer
    public Event newInstance() {
        return new Event();
    }

}
