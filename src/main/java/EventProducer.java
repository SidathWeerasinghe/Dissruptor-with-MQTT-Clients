import com.lmax.disruptor.RingBuffer;

class EventProducer {
    //initialize the ring buffer
    private final RingBuffer<Event> ringBuffer;


    EventProducer(RingBuffer<Event> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    /**
     * Put given messages to the Ring Buffer.
     * @param message Message that put to the disruptor. Contains a single line of a .txt file.
     */
    void onData(String message) {

        // Grab the next sequence
        long sequence = ringBuffer.next();

        try {
            // Get the entry in the Disruptor for the sequence
            Event event = ringBuffer.get(sequence);
            // Fill with data
            event.setValue(message);
        } finally {
            /**
             * Publish sequence whether value set or not in the ring buffer.
             * This will help when there is an interrupt to set value to the ring buffer,
             * But we want to continue the remaining process.
             */
            ringBuffer.publish(sequence);
        }

    }

}