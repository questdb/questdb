package io.questdb.cairo.replication;

import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.ObjectFactory;

public class SequencedQueue<T> {
    public static final <T> SequencedQueue<T> createSingleProducerSingleConsumerQueue(int queueLen, ObjectFactory<T> eventFactory) {
        Sequence producerSeq = new SPSequence(queueLen);
        Sequence consumerSeq = new SCSequence();
        RingQueue<T> queue = new RingQueue<>(eventFactory, queueLen);
        producerSeq.then(consumerSeq).then(producerSeq);
        return new SequencedQueue<T>(producerSeq, consumerSeq, queue);
    }

    public static final <T> SequencedQueue<T> createMultipleProducerSingleConsumerQueue(int queueLen, ObjectFactory<T> eventFactory) {
        Sequence producerSeq = new MPSequence(queueLen);
        Sequence consumerSeq = new SCSequence();
        RingQueue<T> queue = new RingQueue<>(eventFactory, queueLen);
        producerSeq.then(consumerSeq).then(producerSeq);
        return new SequencedQueue<T>(producerSeq, consumerSeq, queue);
    }

    private final Sequence producerSeq;
    private final Sequence consumerSeq;
    private final RingQueue<T> queue;

    SequencedQueue(Sequence producerSeq, Sequence consumerSeq, RingQueue<T> queue) {
        super();
        this.producerSeq = producerSeq;
        this.consumerSeq = consumerSeq;
        this.queue = queue;
    }

    public Sequence getProducerSeq() {
        return producerSeq;
    }

    public Sequence getConsumerSeq() {
        return consumerSeq;
    }

    public T getEvent(long seq) {
        return queue.get(seq);
    }

    public abstract class EnqueueTask {
        public final boolean tryEnqueue() {
            long seq;
            do {
                seq = producerSeq.next();
                if (seq >= 0) {
                    try {
                        T event = queue.get(seq);
                        enqueue(event);
                    } finally {
                        producerSeq.done(seq);
                    }
                }
            } while (seq == -2);

            return false;
        }

        protected abstract void enqueue(T event);
    }
}