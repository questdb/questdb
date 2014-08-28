/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.nfsdb.examples.messaging;

import com.lmax.disruptor.EventFactory;

public class Tick {

    /**
     * Tick object factory for Disruptor.
     */
    public final static EventFactory<Tick> EVENT_FACTORY = new EventFactory<Tick>() {
        @Override
        public Tick newInstance() {
            return new Tick();
        }
    };

    /**
     * Instrument ID. It assumed that instrument IDs are dense 0-based indexes.
     */
    public int instrument;
    // volume in units
    public int volume;
    // price in pence/cent
    public long price;
    /**
     * Bid/Ask indicator. 'b' - bid, 'a' - ask
     */
    public byte bidAsk;
    /**
     * Tick timestamp in milliseconds since 01/01/1970.
     */
    public long timestamp;
}
