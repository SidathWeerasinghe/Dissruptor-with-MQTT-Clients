import com.lmax.disruptor.RingBuffer;

/**
 * Event that handles the inputs of the ring buffer.
 */

class EventProducer {
    //initialize the ring buffer
    private final RingBuffer<MessageEvent> ringBuffer;

    EventProducer(RingBuffer<MessageEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    /**
     * Put given messages to the Ring Buffer.
     * @param message Message that put to the disruptor. Contains a single line of a .txt file.
     */
    void onData(String message) {

        // Grab the next sequence of the ring buffer.
        long sequence = ringBuffer.next();

        try {
            // Get the entry in the Disruptor for the sequence.
            MessageEvent messageEvent = ringBuffer.get(sequence);
            // Fill Disruptor with data.
            messageEvent.setMessage(message);
        } finally {
            // Publish sequence whether value set or not in the ring buffer.
            // This will help when there is an interrupt to set value to the ring buffer,
            // But we want to continue the remaining process.
            ringBuffer.publish(sequence);
        }

    }

}