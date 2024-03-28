/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std;

/*
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

/*
 *
 *
 *
 *
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

import org.jetbrains.annotations.NotNull;

import java.io.ObjectStreamField;
import java.io.Serializable;
import java.lang.ThreadLocal;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongFunction;

/**
 * A hash table supporting full concurrency of retrievals and
 * high expected concurrency for updates. This class obeys the
 * same functional specification as {@link java.util.Hashtable}, and
 * includes versions of methods corresponding to each method of
 * {@code Hashtable}. However, even though all operations are
 * thread-safe, retrieval operations do <em>not</em> entail locking,
 * and there is <em>not</em> any support for locking the entire table
 * in a way that prevents all access.  This class is fully
 * interoperable with {@code Hashtable} in programs that rely on its
 * thread safety but not on its synchronization details.
 * <p>Retrieval operations (including {@code get}) generally do not
 * block, so may overlap with update operations (including {@code put}
 * and {@code remove}). Retrievals reflect the results of the most
 * recently <em>completed</em> update operations holding upon their
 * onset. (More formally, an update operation for a given key bears a
 * <em>happens-before</em> relation with any (non-null) retrieval for
 * that key reporting the updated value.)  For aggregate operations
 * such as {@code putAll} and {@code clear}, concurrent retrievals may
 * reflect insertion or removal of only some entries.  Similarly,
 * Iterators, Spliterators and Enumerations return elements reflecting the
 * state of the hash table at some point at or since the creation of the
 * iterator/enumeration.  They do <em>not</em> throw {@link
 * java.util.ConcurrentModificationException ConcurrentModificationException}.
 * However, iterators are designed to be used by only one thread at a time.
 * Bear in mind that the results of aggregate status methods including
 * {@code size}, {@code isEmpty}, and {@code containsValue} are typically
 * useful only when a map is not undergoing concurrent updates in other threads.
 * Otherwise the results of these methods reflect transient states
 * that may be adequate for monitoring or estimation purposes, but not
 * for program control.
 * <p>The table is dynamically expanded when there are too many
 * collisions (i.e., keys that have distinct hash codes but fall into
 * the same slot modulo the table size), with the expected average
 * effect of maintaining roughly two bins per mapping (corresponding
 * to a 0.75 load factor threshold for resizing). There may be much
 * variance around this average as mappings are added and removed, but
 * overall, this maintains a commonly accepted time/space tradeoff for
 * hash tables.  However, resizing this or any other kind of hash
 * table may be a relatively slow operation. When possible, it is a
 * good idea to provide a size estimate as an optional {@code
 * initialCapacity} constructor argument. An additional optional
 * {@code loadFactor} constructor argument provides a further means of
 * customizing initial table capacity by specifying the table density
 * to be used in calculating the amount of space to allocate for the
 * given number of elements.  Also, for compatibility with previous
 * versions of this class, constructors may optionally specify an
 * expected {@code concurrencyLevel} as an additional hint for
 * internal sizing.  Note that using many keys with exactly the same
 * {@code hashCode()} is a sure way to slow down performance of any
 * hash table. To ameliorate impact, when keys are {@link Comparable},
 * this class may use comparison order among keys to help break ties.
 * <p>A {@link Set} projection of a ConcurrentLongHashMap may be created
 * (using {@link #newKeySet()} or {@link #newKeySet(int)}), or viewed
 * (using {@link #keySet(Object)} when only keys are of interest, and the
 * mapped values are (perhaps transiently) not used or all take the
 * same mapping value.
 * <p> This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
 * interfaces.
 * <p>Like {@link Hashtable} but unlike {@link HashMap}, this class
 * does <em>not</em> allow {@code null} to be used as a key or value.
 * <p>ConcurrentLongHashMap supports a set of sequential and parallel bulk
 * operations that are designed
 * to be safely, and often sensibly, applied even with maps that are
 * being concurrently updated by other threads; for example, when
 * computing a snapshot summary of the values in a shared registry.
 * There are three kinds of operation, each with four forms, accepting
 * functions with Keys, Values, Entries, and (Key, Value) arguments
 * and/or return values. Because the elements of a ConcurrentLongHashMap
 * are not ordered in any particular way, and may be processed in
 * different orders in different parallel executions, the correctness
 * of supplied functions should not depend on any ordering, or on any
 * other objects or values that may transiently change while
 * computation is in progress; and except for forEach actions, should
 * ideally be side-effect-free. Bulk operations on {@link LongEntry}
 * objects do not support method {@code setValue}.
 * <ul>
 * <li> forEach: Perform a given action on each element.
 * A variant form applies a given transformation on each element
 * before performing the action.</li>
 * <li> search: Return the first available non-null result of
 * applying a given function on each element; skipping further
 * search when a result is found.</li>
 * <li> reduce: Accumulate each element.  The supplied reduction
 * function cannot rely on ordering (more formally, it should be
 * both associative and commutative).  There are five variants:
 * <ul>
 * <li> Plain reductions. (There is not a form of this method for
 * (key, value) function arguments since there is no corresponding
 * return type.)</li>
 * <li> Mapped reductions that accumulate the results of a given
 * function applied to each element.</li>
 * <li> Reductions to scalar doubles, longs, and ints, using a
 * given basis value.</li>
 * </ul>
 * </li>
 * </ul>
 * <p>The concurrency properties of bulk operations follow
 * from those of ConcurrentLongHashMap: Any non-null result returned
 * from {@code get(key)} and related access methods bears a
 * happens-before relation with the associated insertion or
 * update.  The result of any bulk operation reflects the
 * composition of these per-element relations (but is not
 * necessarily atomic with respect to the map as a whole unless it
 * is somehow known to be quiescent).  Conversely, because keys
 * and values in the map are never null, null serves as a reliable
 * atomic indicator of the current lack of any result.  To
 * maintain this property, null serves as an implicit basis for
 * all non-scalar reduction operations. For the double, long, and
 * int versions, the basis should be one that, when combined with
 * any other value, returns that other value (more formally, it
 * should be the identity element for the reduction). Most common
 * reductions have these properties; for example, computing a sum
 * with basis 0 or a minimum with basis MAX_VALUE.
 * <p>Search and transformation functions provided as arguments
 * should similarly return null to indicate the lack of any result
 * (in which case it is not used). In the case of mapped
 * reductions, this also enables transformations to serve as
 * filters, returning null (or, in the case of primitive
 * specializations, the identity basis) if the element should not
 * be combined. You can create compound transformations and
 * filterings by composing them yourself under this "null means
 * there is nothing there now" rule before using them in search or
 * reduce operations.
 * <p>Methods accepting and/or returning LongEntry arguments maintain
 * key-value associations. They may be useful for example when
 * finding the key for the greatest value.
 * <p>Bulk operations may complete abruptly, throwing an
 * exception encountered in the application of a supplied
 * function. Bear in mind when handling such exceptions that other
 * concurrently executing functions could also have thrown
 * exceptions, or would have done so if the first exception had
 * not occurred.
 * <p>Speedups for parallel compared to sequential forms are common
 * but not guaranteed.  Parallel operations involving brief functions
 * on small maps may execute more slowly than sequential forms if the
 * underlying work to parallelize the computation is more expensive
 * than the computation itself.  Similarly, parallelization may not
 * lead to much actual parallelism if all processors are busy
 * performing unrelated tasks.
 * <p>All arguments to all task methods must be non-null.
 * <p>This class is a member of the
 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
 * Java Collections Framework</a>.
 *
 * @param <V> the type of mapped values
 * @author Doug Lea
 * @since 1.5
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class ConcurrentLongHashMap<V> implements Serializable {
    static final long EMPTY_KEY = -1;

    /*
     * Overview:
     *
     * The primary design goal of this hash table is to maintain
     * concurrent readability (typically method get(), but also
     * iterators and related methods) while minimizing update
     * contention. Secondary goals are to keep space consumption about
     * the same or better than java.util.HashMap, and to support high
     * initial insertion rates on an empty table by many threads.
     *
     * This map usually acts as a binned (bucketed) hash table.  Each
     * key-value mapping is held in a Node.  Most nodes are instances
     * of the basic Node class with hash, key, value, and next
     * fields. However, various subclasses exist: TreeNodes are
     * arranged in balanced trees, not lists.  TreeBins hold the roots
     * of sets of TreeNodes. ForwardingNodes are placed at the heads
     * of bins during resizing. ReservationNodes are used as
     * placeholders while establishing values in computeIfAbsent and
     * related methods.  The types TreeBin, ForwardingNode, and
     * ReservationNode do not hold normal user keys, values, or
     * hashes, and are readily distinguishable during search etc
     * because they have negative hash fields and null key and value
     * fields. (These special nodes are either uncommon or transient,
     * so the impact of carrying around some unused fields is
     * insignificant.)
     *
     * The table is lazily initialized to a power-of-two size upon the
     * first insertion.  Each bin in the table normally contains a
     * list of Nodes (most often, the list has only zero or one Node).
     * Table accesses require volatile/atomic reads, writes, and
     * CASes.  Because there is no other way to arrange this without
     * adding further indirections, we use intrinsics
     * (sun.misc.Unsafe) operations.
     *
     * We use the top (sign) bit of Node hash fields for control
     * purposes -- it is available anyway because of addressing
     * constraints.  Nodes with negative hash fields are specially
     * handled or ignored in map methods.
     *
     * Insertion (via put or its variants) of the first node in an
     * empty bin is performed by just CASing it to the bin.  This is
     * by far the most common case for put operations under most
     * key/hash distributions.  Other update operations (insert,
     * delete, and replace) require locks.  We do not want to waste
     * the space required to associate a distinct lock object with
     * each bin, so instead use the first node of a bin list itself as
     * a lock. Locking support for these locks relies on builtin
     * "synchronized" monitors.
     *
     * Using the first node of a list as a lock does not by itself
     * suffice though: When a node is locked, any update must first
     * validate that it is still the first node after locking it, and
     * retry if not. Because new nodes are always appended to lists,
     * once a node is first in a bin, it remains first until deleted
     * or the bin becomes invalidated (upon resizing).
     *
     * The main disadvantage of per-bin locks is that other update
     * operations on other nodes in a bin list protected by the same
     * lock can stall, for example when user equals() or mapping
     * functions take a long time.  However, statistically, under
     * random hash codes, this is not a common problem.  Ideally, the
     * frequency of nodes in bins follows a Poisson distribution
     * (http://en.wikipedia.org/wiki/Poisson_distribution) with a
     * parameter of about 0.5 on average, given the resizing threshold
     * of 0.75, although with a large variance because of resizing
     * granularity. Ignoring variance, the expected occurrences of
     * list size k are (exp(-0.5) * pow(0.5, k) / factorial(k)). The
     * first values are:
     *
     * 0:    0.60653066
     * 1:    0.30326533
     * 2:    0.07581633
     * 3:    0.01263606
     * 4:    0.00157952
     * 5:    0.00015795
     * 6:    0.00001316
     * 7:    0.00000094
     * 8:    0.00000006
     * more: less than 1 in ten million
     *
     * Lock contention probability for two threads accessing distinct
     * elements is roughly 1 / (8 * #elements) under random hashes.
     *
     * Actual hash code distributions encountered in practice
     * sometimes deviate significantly from uniform randomness.  This
     * includes the case when N > (1<<30), so some keys MUST collide.
     * Similarly for dumb or hostile usages in which multiple keys are
     * designed to have identical hash codes or ones that differs only
     * in masked-out high bits. So we use a secondary strategy that
     * applies when the number of nodes in a bin exceeds a
     * threshold. These TreeBins use a balanced tree to hold nodes (a
     * specialized form of red-black trees), bounding search time to
     * O(log N).  Each search step in a TreeBin is at least twice as
     * slow as in a regular list, but given that N cannot exceed
     * (1<<64) (before running out of addresses) this bounds search
     * steps, lock hold times, etc, to reasonable constants (roughly
     * 100 nodes inspected per operation worst case) so long as keys
     * are Comparable (which is very common -- String, Long, etc).
     * TreeBin nodes (TreeNodes) also maintain the same "next"
     * traversal pointers as regular nodes, so can be traversed in
     * iterators in the same way.
     *
     * The table is resized when occupancy exceeds a percentage
     * threshold (nominally, 0.75, but see below).  Any thread
     * noticing an overfull bin may assist in resizing after the
     * initiating thread allocates and sets up the replacement array.
     * However, rather than stalling, these other threads may proceed
     * with insertions etc.  The use of TreeBins shields us from the
     * worst case effects of overfilling while resizes are in
     * progress.  Resizing proceeds by transferring bins, one by one,
     * from the table to the next table. However, threads claim small
     * blocks of indices to transfer (via field transferIndex) before
     * doing so, reducing contention.  A generation stamp in field
     * sizeCtl ensures that resizings do not overlap. Because we are
     * using power-of-two expansion, the elements from each bin must
     * either stay at same index, or move with a power of two
     * offset. We eliminate unnecessary node creation by catching
     * cases where old nodes can be reused because their next fields
     * won't change.  On average, only about one-sixth of them need
     * cloning when a table doubles. The nodes they replace will be
     * garbage collectable as soon as they are no longer referenced by
     * any reader thread that may be in the midst of concurrently
     * traversing table.  Upon transfer, the old table bin contains
     * only a special forwarding node (with hash field "MOVED") that
     * contains the next table as its key. On encountering a
     * forwarding node, access and update operations restart, using
     * the new table.
     *
     * Each bin transfer requires its bin lock, which can stall
     * waiting for locks while resizing. However, because other
     * threads can join in and help resize rather than contend for
     * locks, average aggregate waits become shorter as resizing
     * progresses.  The transfer operation must also ensure that all
     * accessible bins in both the old and new table are usable by any
     * traversal.  This is arranged in part by proceeding from the
     * last bin (table.length - 1) up towards the first.  Upon seeing
     * a forwarding node, traversals (see class Traverser) arrange to
     * move to the new table without revisiting nodes.  To ensure that
     * no intervening nodes are skipped even when moved out of order,
     * a stack (see class TableStack) is created on first encounter of
     * a forwarding node during a traversal, to maintain its place if
     * later processing the current table. The need for these
     * save/restore mechanics is relatively rare, but when one
     * forwarding node is encountered, typically many more will be.
     * So Traversers use a simple caching scheme to avoid creating so
     * many new TableStack nodes. (Thanks to Peter Levart for
     * suggesting use of a stack here.)
     *
     * The traversal scheme also applies to partial traversals of
     * ranges of bins (via an alternate Traverser constructor)
     * to support partitioned aggregate operations.  Also, read-only
     * operations give up if ever forwarded to a null table, which
     * provides support for shutdown-style clearing, which is also not
     * currently implemented.
     *
     * Lazy table initialization minimizes footprint until first use,
     * and also avoids resizings when the first operation is from a
     * putAll, constructor with map argument, or deserialization.
     * These cases attempt to override the initial capacity settings,
     * but harmlessly fail to take effect in cases of races.
     *
     * The element count is maintained using a specialization of
     * LongAdder. We need to incorporate a specialization rather than
     * just use a LongAdder in order to access implicit
     * contention-sensing that leads to creation of multiple
     * CounterCells.  The counter mechanics avoid contention on
     * updates but can encounter cache thrashing if read too
     * frequently during concurrent access. To avoid reading so often,
     * resizing under contention is attempted only upon adding to a
     * bin already holding two or more nodes. Under uniform hash
     * distributions, the probability of this occurring at threshold
     * is around 13%, meaning that only about 1 in 8 puts check
     * threshold (and after resizing, many fewer do so).
     *
     * TreeBins use a special form of comparison for search and
     * related operations (which is the main reason we cannot use
     * existing collections such as TreeMaps). TreeBins contain
     * Comparable elements, but may contain others, as well as
     * elements that are Comparable but not necessarily Comparable for
     * the same T, so we cannot invoke compareTo among them. To handle
     * this, the tree is ordered primarily by hash value, then by
     * Comparable.compareTo order if applicable.  On lookup at a node,
     * if elements are not comparable or compare as 0 then both left
     * and right children may need to be searched in the case of tied
     * hash values. (This corresponds to the full list search that
     * would be necessary if all elements were non-Comparable and had
     * tied hashes.) On insertion, to keep a total ordering (or as
     * close as is required here) across rebalancings, we compare
     * classes and identityHashCodes as tie-breakers. The red-black
     * balancing code is updated from pre-jdk-collections
     * (http://gee.cs.oswego.edu/dl/classes/collections/RBCell.java)
     * based in turn on Cormen, Leiserson, and Rivest "Introduction to
     * Algorithms" (CLR).
     *
     * TreeBins also require an additional locking mechanism.  While
     * list traversal is always possible by readers even during
     * updates, tree traversal is not, mainly because of tree-rotations
     * that may change the root node and/or its linkages.  TreeBins
     * include a simple read-write lock mechanism parasitic on the
     * main bin-synchronization strategy: Structural adjustments
     * associated with an insertion or removal are already bin-locked
     * (and so cannot conflict with other writers) but must wait for
     * ongoing readers to finish. Since there can be only one such
     * waiter, we use a simple scheme using a single "waiter" field to
     * block writers.  However, readers need never block.  If the root
     * lock is held, they proceed along the slow traversal path (via
     * next-pointers) until the lock becomes available or the list is
     * exhausted, whichever comes first. These cases are not fast, but
     * maximize aggregate expected throughput.
     *
     * Maintaining API and serialization compatibility with previous
     * versions of this class introduces several oddities. Mainly: We
     * leave untouched but unused constructor arguments referring to
     * concurrencyLevel. We accept a loadFactor constructor argument,
     * but apply it only to initial table capacity (which is the only
     * time that we can guarantee to honor it.) We also declare an
     * unused "Segment" class that is instantiated in minimal form
     * only when serializing.
     *
     * Also, solely for compatibility with previous versions of this
     * class, it extends AbstractMap, even though all of its methods
     * are overridden, so it is just useless baggage.
     *
     * This file is organized to make things a little easier to follow
     * while reading than they might otherwise: First the main static
     * declarations and utilities, then fields, then main public
     * methods (with a few factorings of multiple public methods into
     * internal ones), then sizing methods, trees, traversers, and
     * bulk operations.
     */

    /* ---------------- Constants -------------- */
    static final int HASH_BITS = 0x7fffffff; // usable bits of normal node hash
    /**
     * The largest possible (non-power of two) array size.
     * Needed by toArray and related methods.
     */
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
    /**
     * The smallest table capacity for which bins may be treeified.
     * (Otherwise the table is resized if too many nodes in a bin.)
     * The value should be at least 4 * TREEIFY_THRESHOLD to avoid
     * conflicts between resizing and treeification thresholds.
     */
    static final int MIN_TREEIFY_CAPACITY = 64;
    /*
     * Encodings for Node hash fields. See above for explanation.
     */
    static final int MOVED = -1; // hash for forwarding nodes
    /**
     * Number of CPUS, to place bounds on some sizings
     */
    static final int NCPU = Runtime.getRuntime().availableProcessors();
    static final int RESERVED = -3; // hash for transient reservations
    static final int TREEBIN = -2; // hash for roots of trees
    /**
     * The bin count threshold for using a tree rather than list for a
     * bin.  Bins are converted to trees when adding an element to a
     * bin with at least this many nodes. The value must be greater
     * than 2, and should be at least 8 to mesh with assumptions in
     * tree removal about conversion back to plain bins upon
     * shrinkage.
     */
    static final int TREEIFY_THRESHOLD = 8;
    /**
     * The bin count threshold for untreeifying a (split) bin during a
     * resize operation. Should be less than TREEIFY_THRESHOLD, and at
     * most 6 to mesh with shrinkage detection under removal.
     */
    static final int UNTREEIFY_THRESHOLD = 6;
    /* ---------------- Fields -------------- */
    private static final long ABASE;
    private static final int ASHIFT;
    /*
     * Volatile access methods are used for table elements as well as
     * elements of in-progress next table while resizing.  All uses of
     * the tab arguments must be null checked by callers.  All callers
     * also paranoically precheck that tab's length is not zero (or an
     * equivalent check), thus ensuring that any index argument taking
     * the form of a hash value anded with (length - 1) is a valid
     * index.  Note that, to be correct wrt arbitrary concurrency
     * errors by users, these checks must operate on local variables,
     * which accounts for some odd-looking inline assignments below.
     * Note that calls to setTabAt always occur within locked regions,
     * and so in principle require only release ordering, not
     * full volatile semantics, but are currently coded as volatile
     * writes to be conservative.
     */
    private static final long BASECOUNT;
    private static final long CELLSBUSY;
    private static final long CELLVALUE;
    /**
     * The default initial table capacity.  Must be a power of 2
     * (i.e., at least 1) and at most MAXIMUM_CAPACITY.
     */
    private static final int DEFAULT_CAPACITY = 16;
    /**
     * The load factor for this table. Overrides of this value in
     * constructors affect only the initial table capacity.  The
     * actual floating point value isn't normally used -- it is
     * simpler to use expressions such as {@code n - (n >>> 2)} for
     * the associated resizing threshold.
     */
    private static final float LOAD_FACTOR = 0.75f;
    /**
     * The largest possible table capacity.  This value must be
     * exactly 1<<30 to stay within Java array allocation and indexing
     * bounds for power of two table sizes, and is further required
     * because the top two bits of 32bit hash fields are used for
     * control purposes.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;
    /**
     * Minimum number of rebinnings per transfer step. Ranges are
     * subdivided to allow multiple resizer threads.  This value
     * serves as a lower bound to avoid resizers encountering
     * excessive memory contention.  The value should be at least
     * DEFAULT_CAPACITY.
     */
    private static final int MIN_TRANSFER_STRIDE = 16;
    private static final long PROBE;

    /* ---------------- Nodes -------------- */
    /**
     * The increment for generating probe values
     */
    private static final int PROBE_INCREMENT = 0x9e3779b9;

    /* ---------------- Static utilities -------------- */
    /**
     * The number of bits used for generation stamp in sizeCtl.
     * Must be at least 6 for 32bit arrays.
     */
    private static final int RESIZE_STAMP_BITS = 16;
    /**
     * The maximum number of threads that can help resize.
     * Must fit in 32 - RESIZE_STAMP_BITS bits.
     */
    private static final int MAX_RESIZERS = (1 << (32 - RESIZE_STAMP_BITS)) - 1;
    /**
     * The bit shift for recording size stamp in sizeCtl.
     */
    private static final int RESIZE_STAMP_SHIFT = 32 - RESIZE_STAMP_BITS;
    private static final long SEED;

    /* ---------------- Table element access -------------- */
    /**
     * The increment of seeder per new instance
     */
    private static final long SEEDER_INCREMENT = 0xbb67ae8584caa73bL;
    private static final long SIZECTL;
    private static final long TRANSFERINDEX;
    /**
     * Generates per-thread initialization/probe field
     */
    private static final AtomicInteger probeGenerator = new AtomicInteger();
    /**
     * The next seed for default constructors.
     */
    private static final AtomicLong seeder = new AtomicLong(initialSeed());
    /**
     * For serialization compatibility.
     */
    private static final ObjectStreamField[] serialPersistentFields = {
            new ObjectStreamField("segments", Segment[].class),
            new ObjectStreamField("segmentMask", Integer.TYPE),
            new ObjectStreamField("segmentShift", Integer.TYPE)
    };
    private static final long serialVersionUID = 7249069246763182397L;
    private final java.lang.ThreadLocal<Traverser<V>> tlTraverser = ThreadLocal.withInitial(Traverser::new);
    /**
     * The array of bins. Lazily initialized upon first insertion.
     * Size is always a power of two. Accessed directly by iterators.
     */
    transient volatile Node<V>[] table;
    /**
     * Base counter value, used mainly when there is no contention,
     * but also as a fallback during table initialization
     * races. Updated via CAS.
     */
    private transient volatile long baseCount;
    /**
     * Spinlock (locked via CAS) used when resizing and/or creating CounterCells.
     */
    private transient volatile int cellsBusy;
    /**
     * Table of counter cells. When non-null, size is a power of 2.
     */
    private transient volatile CounterCell[] counterCells;
    // Original (since JDK1.2) Map methods
    private transient EntrySetView<V> entrySet;
    /* ---------------- Public operations -------------- */
    // views
    private transient KeySetView<V> keySet;
    /**
     * The next table to use; non-null only while resizing.
     */
    private transient volatile Node<V>[] nextTable;
    /**
     * Table initialization and resizing control.  When negative, the
     * table is being initialized or resized: -1 for initialization,
     * else -(1 + the number of active resizing threads).  Otherwise,
     * when table is null, holds the initial table size to use upon
     * creation, or 0 for default. After initialization, holds the
     * next element count value upon which to resize the table.
     */
    private transient volatile int sizeCtl;
    /**
     * The next table index (plus one) to split while resizing.
     */
    private transient volatile int transferIndex;
    private transient ValuesView<V> values;

    /**
     * Creates a new, empty map with an initial table size based on
     * the given number of elements ({@code initialCapacity}), table
     * density ({@code loadFactor}), and number of concurrently
     * updating threads ({@code concurrencyLevel}).
     *
     * @param initialCapacity the initial capacity. The implementation
     *                        performs internal sizing to accommodate this many elements,
     *                        given the specified load factor.
     * @param loadFactor      the load factor (table density) for
     *                        establishing the initial table size
     * @throws IllegalArgumentException if the initial capacity is
     *                                  negative or the load factor or concurrencyLevel are
     *                                  nonpositive
     */
    public ConcurrentLongHashMap(int initialCapacity, float loadFactor) {
        if (!(loadFactor > 0.0f) || initialCapacity < 0)
            throw new IllegalArgumentException();
        if (initialCapacity < 1)   // Use at least as many bins
            initialCapacity = 1;   // as estimated threads
        long size = (long) (1.0 + (long) initialCapacity / loadFactor);
        this.sizeCtl = (size >= (long) MAXIMUM_CAPACITY) ?
                MAXIMUM_CAPACITY : tableSizeFor((int) size);
    }

    /**
     * Creates a new map with the same mappings as the given map.
     *
     * @param m the map
     */
    public ConcurrentLongHashMap(ConcurrentLongHashMap<? extends V> m) {
        this.sizeCtl = DEFAULT_CAPACITY;
        putAll(m);
    }

    /**
     * Creates a new, empty map with the default initial table size (16).
     */
    public ConcurrentLongHashMap() {
    }

    /**
     * Creates a new, empty map with an initial table size
     * accommodating the specified number of elements without the need
     * to dynamically resize.
     *
     * @param initialCapacity The implementation performs internal
     *                        sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     *                                  elements is negative
     */
    public ConcurrentLongHashMap(int initialCapacity) {
        if (initialCapacity < 0)
            throw new IllegalArgumentException();
        this.sizeCtl = ((initialCapacity >= (MAXIMUM_CAPACITY >>> 1)) ?
                MAXIMUM_CAPACITY :
                tableSizeFor(initialCapacity + (initialCapacity >>> 1) + 1));
    }

    /**
     * Creates a new {@link Set} backed by a ConcurrentLongHashMap
     * from the given type to {@code Boolean.TRUE}.
     *
     * @return the new set
     * @since 1.8
     */
    public static KeySetView<Boolean> newKeySet() {
        return new KeySetView<>(new ConcurrentLongHashMap<>(), Boolean.TRUE);
    }

    /**
     * Creates a new {@link Set} backed by a ConcurrentLongHashMap
     * from the given type to {@code Boolean.TRUE}.
     *
     * @param initialCapacity The implementation performs internal
     *                        sizing to accommodate this many elements.
     * @return the new set
     * @throws IllegalArgumentException if the initial capacity of
     *                                  elements is negative
     * @since 1.8
     */
    public static KeySetView<Boolean> newKeySet(int initialCapacity) {
        return new KeySetView<>(new ConcurrentLongHashMap<>(initialCapacity), Boolean.TRUE);
    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
        long delta = 0L; // negative number of deletions
        int i = 0;
        Node<V>[] tab = table;
        while (tab != null && i < tab.length) {
            int fh;
            Node<V> f = tabAt(tab, i);
            if (f == null)
                ++i;
            else if ((fh = f.hash) == MOVED) {
                tab = helpTransfer(tab, f);
                i = 0; // restart
            } else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        Node<V> p = (fh >= 0 ? f :
                                (f instanceof TreeBin) ?
                                        ((TreeBin<V>) f).first : null);
                        while (p != null) {
                            --delta;
                            p = p.next;
                        }
                        setTabAt(tab, i++, null);
                    }
                }
            }
        }
        if (delta != 0L)
            addCount(delta, -1);
    }

    /**
     * Attempts to compute a mapping for the specified key and its
     * current mapped value (or {@code null} if there is no current
     * mapping). The entire method invocation is performed atomically.
     * Some attempted update operations on this map by other threads
     * may be blocked while computation is in progress, so the
     * computation should be short and simple, and must not attempt to
     * update any other mappings of this Map.
     *
     * @param key               key with which the specified value is to be associated
     * @param remappingFunction the function to compute a value
     * @return the new value associated with the specified key, or null if none
     * @throws IllegalArgumentException if the specified key is negative
     * @throws NullPointerException     if the specified remappingFunction is null
     * @throws IllegalStateException    if the computation detectably
     *                                  attempts a recursive update to this map that would
     *                                  otherwise never complete
     * @throws RuntimeException         or Error if the remappingFunction does so,
     *                                  in which case the mapping is unchanged
     */
    public V compute(long key, BiLongFunction<? super V, ? extends V> remappingFunction) {
        if (key < 0)
            throw new IllegalArgumentException();
        if (remappingFunction == null)
            throw new NullPointerException();
        int h = spread(keyHashCode(key));
        V val = null;
        int delta = 0;
        int binCount = 0;
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                Node<V> r = new ReservationNode<V>();
                synchronized (r) {
                    if (casTabAt(tab, i, r)) {
                        binCount = 1;
                        Node<V> node = null;
                        try {
                            if ((val = remappingFunction.apply(key, null)) != null) {
                                delta = 1;
                                node = new Node<V>(h, key, val, null);
                            }
                        } finally {
                            setTabAt(tab, i, node);
                        }
                    }
                }
                if (binCount != 0)
                    break;
            } else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f, pred = null; ; ++binCount) {
                                if (e.hash == h && (e.key == key)) {
                                    val = remappingFunction.apply(key, e.val);
                                    if (val != null)
                                        e.val = val;
                                    else {
                                        delta = -1;
                                        Node<V> en = e.next;
                                        if (pred != null)
                                            pred.next = en;
                                        else
                                            setTabAt(tab, i, en);
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null) {
                                    val = remappingFunction.apply(key, null);
                                    if (val != null) {
                                        delta = 1;
                                        pred.next = new Node<V>(h, key, val, null);
                                    }
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            binCount = 1;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r, p;
                            if ((r = t.root) != null)
                                p = r.findTreeNode(h, key);
                            else
                                p = null;
                            V pv = (p == null) ? null : p.val;
                            val = remappingFunction.apply(key, pv);
                            if (val != null) {
                                if (p != null)
                                    p.val = val;
                                else {
                                    delta = 1;
                                    t.putTreeVal(h, key, val);
                                }
                            } else if (p != null) {
                                delta = -1;
                                if (t.removeTreeNode(p))
                                    setTabAt(tab, i, untreeify(t.first));
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD)
                        treeifyBin(tab, i);
                    break;
                }
            }
        }
        if (delta != 0)
            addCount(delta, binCount);
        return val;
    }

    /**
     * If the specified key is not already associated with a value,
     * attempts to compute its value using the given mapping function
     * and enters it into this map unless {@code null}.  The entire
     * method invocation is performed atomically, so the function is
     * applied at most once per key.  Some attempted update operations
     * on this map by other threads may be blocked while computation
     * is in progress, so the computation should be short and simple,
     * and must not attempt to update any other mappings of this map.
     *
     * @param key             key with which the specified value is to be associated
     * @param token           token to pass to the mapping function
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value associated with
     * the specified key, or null if the computed value is null
     * @throws IllegalArgumentException if the specified key is negative
     * @throws NullPointerException     if the specified mappingFunction is null
     * @throws IllegalStateException    if the computation detectably
     *                                  attempts a recursive update to this map that would
     *                                  otherwise never complete
     * @throws RuntimeException         or Error if the mappingFunction does so,
     *                                  in which case the mapping is left unestablished
     */
    public V computeIfAbsent(long key, Object token, BiLongFunction<Object, ? extends V> mappingFunction) {
        if (key < 0)
            throw new IllegalArgumentException();
        if (mappingFunction == null)
            throw new NullPointerException();
        int h = spread(keyHashCode(key));
        V val = null;
        int binCount = 0;
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                Node<V> r = new ReservationNode<>();
                synchronized (r) {
                    if (casTabAt(tab, i, r)) {
                        binCount = 1;
                        Node<V> node = null;
                        try {
                            if ((val = mappingFunction.apply(key, token)) != null)
                                node = new Node<>(h, key, val, null);
                        } finally {
                            setTabAt(tab, i, node);
                        }
                    }
                }
                if (binCount != 0)
                    break;
            } else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                boolean added = false;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f; ; ++binCount) {
                                if (e.hash == h && e.key == key) {
                                    val = e.val;
                                    break;
                                }
                                Node<V> pred = e;
                                if ((e = e.next) == null) {
                                    if ((val = mappingFunction.apply(key, token)) != null) {
                                        added = true;
                                        pred.next = new Node<>(h, key, val, null);
                                    }
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            binCount = 2;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r, p;
                            if ((r = t.root) != null &&
                                    (p = r.findTreeNode(h, key)) != null)
                                val = p.val;
                            else if ((val = mappingFunction.apply(key, token)) != null) {
                                added = true;
                                t.putTreeVal(h, key, val);
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD)
                        treeifyBin(tab, i);
                    if (!added)
                        return val;
                    break;
                }
            }
        }
        if (val != null)
            addCount(1L, binCount);
        return val;
    }

    /**
     * If the specified key is not already associated with a value,
     * attempts to compute its value using the given mapping function
     * and enters it into this map unless {@code null}.  The entire
     * method invocation is performed atomically, so the function is
     * applied at most once per key.  Some attempted update operations
     * on this map by other threads may be blocked while computation
     * is in progress, so the computation should be short and simple,
     * and must not attempt to update any other mappings of this map.
     *
     * @param key             key with which the specified value is to be associated
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value associated with
     * the specified key, or null if the computed value is null
     * @throws IllegalArgumentException if the specified key is negative
     * @throws NullPointerException     if the specified key or mappingFunction
     *                                  is null
     * @throws IllegalStateException    if the computation detectably
     *                                  attempts a recursive update to this map that would
     *                                  otherwise never complete
     * @throws RuntimeException         or Error if the mappingFunction does so,
     *                                  in which case the mapping is left unestablished
     */
    public V computeIfAbsent(long key, LongFunction<? extends V> mappingFunction) {
        if (key < 0)
            throw new IllegalArgumentException();
        if (mappingFunction == null)
            throw new NullPointerException();
        int h = spread(keyHashCode(key));
        V val = null;
        int binCount = 0;
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                Node<V> r = new ReservationNode<V>();
                synchronized (r) {
                    if (casTabAt(tab, i, r)) {
                        binCount = 1;
                        Node<V> node = null;
                        try {
                            if ((val = mappingFunction.apply(key)) != null)
                                node = new Node<V>(h, key, val, null);
                        } finally {
                            setTabAt(tab, i, node);
                        }
                    }
                }
                if (binCount != 0)
                    break;
            } else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                boolean added = false;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f; ; ++binCount) {
                                if (e.hash == h && e.key == key) {
                                    val = e.val;
                                    break;
                                }
                                Node<V> pred = e;
                                if ((e = e.next) == null) {
                                    if ((val = mappingFunction.apply(key)) != null) {
                                        added = true;
                                        pred.next = new Node<V>(h, key, val, null);
                                    }
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            binCount = 2;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r, p;
                            if ((r = t.root) != null &&
                                    (p = r.findTreeNode(h, key)) != null)
                                val = p.val;
                            else if ((val = mappingFunction.apply(key)) != null) {
                                added = true;
                                t.putTreeVal(h, key, val);
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD)
                        treeifyBin(tab, i);
                    if (!added)
                        return val;
                    break;
                }
            }
        }
        if (val != null)
            addCount(1L, binCount);
        return val;
    }

    /**
     * If the value for the specified key is present, attempts to
     * compute a new mapping given the key and its current mapped
     * value.  The entire method invocation is performed atomically.
     * Some attempted update operations on this map by other threads
     * may be blocked while computation is in progress, so the
     * computation should be short and simple, and must not attempt to
     * update any other mappings of this map.
     *
     * @param key               key with which a value may be associated
     * @param remappingFunction the function to compute a value
     * @return the new value associated with the specified key, or null if none
     * @throws IllegalArgumentException if the specified key is negative
     * @throws NullPointerException     if the specified remappingFunction is null
     * @throws IllegalStateException    if the computation detectably
     *                                  attempts a recursive update to this map that would
     *                                  otherwise never complete
     * @throws RuntimeException         or Error if the remappingFunction does so,
     *                                  in which case the mapping is unchanged
     */
    public V computeIfPresent(long key, BiLongFunction<? super V, ? extends V> remappingFunction) {
        if (key < 0)
            throw new IllegalArgumentException();
        if (remappingFunction == null)
            throw new NullPointerException();
        int h = spread(keyHashCode(key));
        V val = null;
        int delta = 0;
        int binCount = 0;
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & h)) == null)
                break;
            else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f, pred = null; ; ++binCount) {
                                if (e.hash == h && e.key == key) {
                                    val = remappingFunction.apply(key, e.val);
                                    if (val != null)
                                        e.val = val;
                                    else {
                                        delta = -1;
                                        Node<V> en = e.next;
                                        if (pred != null)
                                            pred.next = en;
                                        else
                                            setTabAt(tab, i, en);
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null)
                                    break;
                            }
                        } else if (f instanceof TreeBin) {
                            binCount = 2;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r, p;
                            if ((r = t.root) != null &&
                                    (p = r.findTreeNode(h, key)) != null) {
                                val = remappingFunction.apply(key, p.val);
                                if (val != null)
                                    p.val = val;
                                else {
                                    delta = -1;
                                    if (t.removeTreeNode(p))
                                        setTabAt(tab, i, untreeify(t.first));
                                }
                            }
                        }
                    }
                }
                if (binCount != 0)
                    break;
            }
        }
        if (delta != 0)
            addCount(delta, binCount);
        return val;
    }

    /**
     * Tests if the specified object is a key in this table.
     *
     * @param key possible key
     * @return {@code true} if and only if the specified object
     * is a key in this table, as determined by the
     * {@code equals} method; {@code false} otherwise
     * @throws NullPointerException if the specified key is null
     */
    public boolean containsKey(long key) {
        return get(key) != null;
    }

    /**
     * Returns {@code true} if this map maps one or more keys to the
     * specified value. Note: This method may require a full traversal
     * of the map, and is much slower than method {@code containsKey}.
     *
     * @param value value whose presence in this map is to be tested
     * @return {@code true} if this map maps one or more keys to the
     * specified value
     * @throws NullPointerException if the specified value is null
     */
    public boolean containsValue(V value) {
        if (value == null)
            throw new NullPointerException();
        Node<V>[] t = table;
        if (t != null) {
            Traverser<V> it = getTraverser(t);
            for (Node<V> p; (p = it.advance()) != null; ) {
                V v;
                if ((v = p.val) == value || (value.equals(v)))
                    return true;
            }
        }
        return false;
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from the map,
     * via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear}
     * operations.
     * <p>The view's iterators and spliterators are
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * @return the set view
     */
    @NotNull
    public Set<LongEntry<V>> entrySet() {
        EntrySetView<V> es;
        return (es = entrySet) != null ? es : (entrySet = new EntrySetView<>(this));
    }

    /**
     * Compares the specified object with this map for equality.
     * Returns {@code true} if the given object is a map with the same
     * mappings as this map.  This operation may return misleading
     * results if either map is concurrently modified during execution
     * of this method.
     *
     * @param o object to be compared for equality with this map
     * @return {@code true} if the specified object is equal to this map
     */
    public boolean equals(Object o) {
        if (o != this) {
            if (!(o instanceof ConcurrentLongHashMap))
                return false;
            ConcurrentLongHashMap<?> m = (ConcurrentLongHashMap<?>) o;
            Traverser<V> it = getTraverser(table);
            for (Node<V> p; (p = it.advance()) != null; ) {
                V val = p.val;
                Object v = m.get(p.key);
                if (v == null || (v != val && !v.equals(val)))
                    return false;
            }
            for (LongEntry<?> e : m.entrySet()) {
                long mk;
                Object mv, v;
                if ((mk = e.getKey()) == EMPTY_KEY ||
                        (mv = e.getValue()) == null ||
                        (v = get(mk)) == null ||
                        (mv != v && !mv.equals(v)))
                    return false;
            }
        }
        return true;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code key.equals(k)},
     * then this method returns {@code v}; otherwise it returns
     * {@code null}.  (There can be at most one such mapping.)
     *
     * @param key map key value
     * @return value to which specified key is mapped
     * @throws NullPointerException if the specified key is null
     */
    public V get(long key) {
        Node<V>[] tab;
        Node<V> e, p;
        int n, eh;
        int h = spread(keyHashCode(key));
        if ((tab = table) != null && (n = tab.length) > 0 &&
                (e = tabAt(tab, (n - 1) & h)) != null) {
            if ((eh = e.hash) == h) {
                if (e.key == key)
                    return e.val;
            } else if (eh < 0)
                return (p = e.find(h, key)) != null ? p.val : null;
            while ((e = e.next) != null) {
                if (e.hash == h && e.key == key)
                    return e.val;
            }
        }
        return null;
    }

    /**
     * Returns the value to which the specified key is mapped, or the
     * given default value if this map contains no mapping for the
     * key.
     *
     * @param key          the key whose associated value is to be returned
     * @param defaultValue the value to return if this map contains
     *                     no mapping for the given key
     * @return the mapping for the key, if present; else the default value
     * @throws NullPointerException if the specified key is null
     */
    public V getOrDefault(long key, V defaultValue) {
        V v;
        return (v = get(key)) == null ? defaultValue : v;
    }

    /**
     * Returns the hash code value for this {@link Map}, i.e.,
     * the sum of, for each key-value pair in the map,
     * {@code key.hashCode() ^ value.hashCode()}.
     *
     * @return the hash code value for this map
     */
    public int hashCode() {
        int h = 0;
        Node<V>[] t = table;
        if (t != null) {
            Traverser<V> it = getTraverser(t);
            for (Node<V> p; (p = it.advance()) != null; )
                h += keyHashCode(p.key) ^ p.val.hashCode();
        }
        return h;
    }

    // ConcurrentMap methods

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty() {
        return sumCount() <= 0L; // ignore transient negative values
    }

    /**
     * Returns a {@link Set} view of the keys in this map, using the
     * given common mapped value for any additions (i.e., {@link
     * Collection#add} and {@link Collection#addAll(Collection)}).
     * This is of course only appropriate if it is acceptable to use
     * the same value for all additions from this view.
     *
     * @param mappedValue the mapped value to use for any additions
     * @return the set view
     * @throws NullPointerException if the mappedValue is null
     */
    public KeySetView<V> keySet(V mappedValue) {
        if (mappedValue == null)
            throw new NullPointerException();
        return new KeySetView<>(this, mappedValue);
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa. The set supports element
     * removal, which removes the corresponding mapping from this map,
     * via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear}
     * operations.  It does not support the {@code add} or
     * {@code addAll} operations.
     * <p>The view's iterators and spliterators are
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     * <p>
     *
     * @return the set view
     */
    @NotNull
    public KeySetView<V> keySet() {
        KeySetView<V> ks;
        return (ks = keySet) != null ? ks : (keySet = new KeySetView<>(this, null));
    }

    /**
     * Returns the number of mappings. This method should be used
     * instead of {@link #size} because a ConcurrentLongHashMap may
     * contain more mappings than can be represented as an int. The
     * value returned is an estimate; the actual count may differ if
     * there are concurrent insertions or removals.
     *
     * @return the number of mappings
     * @since 1.8
     */
    public long mappingCount() {
        return Math.max(sumCount(), 0L); // ignore transient negative values
    }

    // Overrides of JDK8+ Map extension method defaults

    /**
     * Maps the specified key to the specified value in this table.
     * Neither the key nor the value can be null.
     * <p>The value can be retrieved by calling the {@code get} method
     * with a key that is equal to the original key.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with {@code key}, or
     * {@code null} if there was no mapping for {@code key}
     * @throws NullPointerException if the specified key or value is null
     */
    public V put(long key, V value) {
        return putVal(key, value, false);
    }

    /**
     * Copies all of the mappings from the specified map to this one.
     * These mappings replace any mappings that this map had for any of the
     * keys currently in the specified map.
     *
     * @param m mappings to be stored in this map
     */
    public void putAll(@NotNull ConcurrentLongHashMap<? extends V> m) {
        tryPresize(m.size());
        for (LongEntry<? extends V> e : m.entrySet())
            putVal(e.getKey(), e.getValue(), false);
    }

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     * or {@code null} if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V putIfAbsent(long key, V value) {
        return putVal(key, value, true);
    }

    public boolean remove(long key, V value) {
        return value != null && replaceNode(key, null, value) != null;
    }

    /**
     * Removes the key (and its corresponding value) from this map.
     * This method does nothing if the key is not in the map.
     *
     * @param key the key that needs to be removed
     * @return the previous value associated with {@code key}, or
     * {@code null} if there was no mapping for {@code key}
     * @throws NullPointerException if the specified key is null
     */
    public V remove(long key) {
        return replaceNode(key, null, null);
    }

    // Hashtable legacy methods

    public boolean replace(long key, @NotNull V oldValue, @NotNull V newValue) {
        return replaceNode(key, newValue, oldValue) != null;
    }

    // ConcurrentLongHashMap-only methods

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     * or {@code null} if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V replace(long key, @NotNull V value) {
        return replaceNode(key, value, null);
    }

    /**
     * {@inheritDoc}
     */
    public int size() {
        long n = sumCount();
        return ((n < 0L) ? 0 :
                (n > (long) Integer.MAX_VALUE) ? Integer.MAX_VALUE :
                        (int) n);
    }

    /**
     * Returns a string representation of this map.  The string
     * representation consists of a list of key-value mappings (in no
     * particular order) enclosed in braces ("{@code {}}").  Adjacent
     * mappings are separated by the characters {@code ", "} (comma
     * and space).  Each key-value mapping is rendered as the key
     * followed by an equals sign ("{@code =}") followed by the
     * associated value.
     *
     * @return a string representation of this map
     */
    public String toString() {
        Traverser<V> it = getTraverser(table);
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        Node<V> p;
        if ((p = it.advance()) != null) {
            for (; ; ) {
                long k = p.key;
                V v = p.val;
                sb.append(k);
                sb.append('=');
                sb.append(v == this ? "(this Map)" : v);
                if ((p = it.advance()) == null)
                    break;
                sb.append(',').append(' ');
            }
        }
        return sb.append('}').toString();
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  The collection
     * supports element removal, which removes the corresponding
     * mapping from this map, via the {@code Iterator.remove},
     * {@code Collection.remove}, {@code removeAll},
     * {@code retainAll}, and {@code clear} operations.  It does not
     * support the {@code add} or {@code addAll} operations.
     * <p>The view's iterators and spliterators are
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * @return the collection view
     */
    @NotNull
    public Collection<V> values() {
        ValuesView<V> vs;
        return (vs = values) != null ? vs : (values = new ValuesView<>(this));
    }

    /* ---------------- Special Nodes -------------- */

    private static long initialSeed() {
        String pp = System.getProperty("java.util.secureRandomSeed");

        if (pp != null && pp.equalsIgnoreCase("true")) {
            byte[] seedBytes = java.security.SecureRandom.getSeed(8);
            long s = (long) (seedBytes[0]) & 0xffL;
            for (int i = 1; i < 8; ++i)
                s = (s << 8) | ((long) (seedBytes[i]) & 0xffL);
            return s;
        }
        return (mix64(System.currentTimeMillis()) ^
                mix64(System.nanoTime()));
    }

    /* ---------------- Table Initialization and Resizing -------------- */

    private static int keyHashCode(final long key) {
        return Hash.hashLong32(key);
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    /**
     * Returns a power of two table size for the given desired capacity.
     * See Hackers Delight, sec 3.2
     */
    private static int tableSizeFor(int c) {
        int n = c - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    /**
     * Adds to count, and if table is too small and not already
     * resizing, initiates transfer. If already resizing, helps
     * perform transfer if work is available.  Rechecks occupancy
     * after a transfer to see if another resize is already needed
     * because resizings are lagging additions.
     *
     * @param x     the count to add
     * @param check if <0, don't check resize, if <= 1 only check if uncontended
     */
    private void addCount(long x, int check) {
        CounterCell[] as;
        long b, s;
        if ((as = counterCells) != null || !Unsafe.cas(this, BASECOUNT, b = baseCount, s = b + x)) {
            CounterCell a;
            long v;
            int m;
            boolean uncontended = true;
            if (as == null || (m = as.length - 1) < 0 ||
                    (a = as[getProbe() & m]) == null ||
                    !(uncontended = Unsafe.cas(a, CELLVALUE, v = a.value, v + x))) {
                fullAddCount(x, uncontended);
                return;
            }
            if (check <= 1)
                return;
            s = sumCount();
        }
        if (check >= 0) {
            Node<V>[] tab, nt;
            int n, sc;
            while (s >= (long) (sc = sizeCtl) && (tab = table) != null &&
                    (n = tab.length) < MAXIMUM_CAPACITY) {
                int rs = resizeStamp(n);
                if (sc < 0) {
                    if (sc >>> RESIZE_STAMP_SHIFT != rs || sc == rs + MAX_RESIZERS || (nt = nextTable) == null || transferIndex <= 0)
                        break;
                    if (Unsafe.getUnsafe().compareAndSwapInt(this, SIZECTL, sc, sc + 1))
                        transfer(tab, nt);
                } else if (Unsafe.getUnsafe().compareAndSwapInt(this, SIZECTL, sc,
                        (rs << RESIZE_STAMP_SHIFT) + 2))
                    transfer(tab, null);
                s = sumCount();
            }
        }
    }

    // See LongAdder version for explanation
    private void fullAddCount(long x, boolean wasUncontended) {
        int h;
        if ((h = getProbe()) == 0) {
            localInit();      // force initialization
            h = getProbe();
            wasUncontended = true;
        }
        boolean collide = false;                // True if last slot nonempty
        for (; ; ) {
            CounterCell[] as;
            CounterCell a;
            int n;
            long v;
            if ((as = counterCells) != null && (n = as.length) > 0) {
                if ((a = as[(n - 1) & h]) == null) {
                    if (cellsBusy == 0) {            // Try to attach new Cell
                        CounterCell r = new CounterCell(x); // Optimistic create
                        if (cellsBusy == 0 &&
                                Unsafe.getUnsafe().compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                            boolean created = false;
                            try {               // Recheck under lock
                                CounterCell[] rs;
                                int m, j;
                                if ((rs = counterCells) != null &&
                                        (m = rs.length) > 0 &&
                                        rs[j = (m - 1) & h] == null) {
                                    rs[j] = r;
                                    created = true;
                                }
                            } finally {
                                cellsBusy = 0;
                            }
                            if (created)
                                break;
                            continue;           // Slot is now non-empty
                        }
                    }
                    collide = false;
                } else if (!wasUncontended)       // CAS already known to fail
                    wasUncontended = true;      // Continue after rehash
                else if (Unsafe.cas(a, CELLVALUE, v = a.value, v + x))
                    break;
                else if (counterCells != as || n >= NCPU)
                    collide = false;            // At max size or stale
                else if (!collide)
                    collide = true;
                else if (cellsBusy == 0 &&
                        Unsafe.getUnsafe().compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                    try {
                        if (counterCells == as) {// Expand table unless stale
                            CounterCell[] rs = new CounterCell[n << 1];
                            System.arraycopy(as, 0, rs, 0, n);
                            counterCells = rs;
                        }
                    } finally {
                        cellsBusy = 0;
                    }
                    collide = false;
                    continue;                   // Retry with expanded table
                }
                h = advanceProbe(h);
            } else if (cellsBusy == 0 && counterCells == as &&
                    Unsafe.getUnsafe().compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                boolean init = false;
                try {                           // Initialize table
                    if (counterCells == as) {
                        CounterCell[] rs = new CounterCell[2];
                        rs[h & 1] = new CounterCell(x);
                        counterCells = rs;
                        init = true;
                    }
                } finally {
                    cellsBusy = 0;
                }
                if (init)
                    break;
            } else if (Unsafe.cas(this, BASECOUNT, v = baseCount, v + x))
                break;                          // Fall back on using base
        }
    }

    private Traverser<V> getTraverser(Node<V>[] tab) {
        Traverser<V> traverser = tlTraverser.get();
        int len = tab == null ? 0 : tab.length;
        traverser.of(tab, len, len);
        return traverser;
    }

    /**
     * Initializes table, using the size recorded in sizeCtl.
     */
    private Node<V>[] initTable() {
        Node<V>[] tab;
        int sc;
        while ((tab = table) == null || tab.length == 0) {
            if ((sc = sizeCtl) < 0)
                Os.pause(); // lost initialization race; just spin
            else if (Unsafe.getUnsafe().compareAndSwapInt(this, SIZECTL, sc, -1)) {
                try {
                    if ((tab = table) == null || tab.length == 0) {
                        int n = (sc > 0) ? sc : DEFAULT_CAPACITY;
                        @SuppressWarnings("unchecked")
                        Node<V>[] nt = (Node<V>[]) new Node<?>[n];
                        table = tab = nt;
                        sc = n - (n >>> 2);
                    }
                } finally {
                    sizeCtl = sc;
                }
                break;
            }
        }
        return tab;
    }

    /**
     * Moves and/or copies the nodes in each bin to new table. See
     * above for explanation.
     */
    private void transfer(Node<V>[] tab, Node<V>[] nextTab) {
        int n = tab.length, stride;
        if ((stride = (NCPU > 1) ? (n >>> 3) / NCPU : n) < MIN_TRANSFER_STRIDE)
            stride = MIN_TRANSFER_STRIDE; // subdivide range
        if (nextTab == null) {            // initiating
            try {
                @SuppressWarnings("unchecked")
                Node<V>[] nt = (Node<V>[]) new Node<?>[n << 1];
                nextTab = nt;
            } catch (Throwable ex) {      // try to cope with OOME
                sizeCtl = Integer.MAX_VALUE;
                return;
            }
            nextTable = nextTab;
            transferIndex = n;
        }
        int nextn = nextTab.length;
        ForwardingNode<V> fwd = new ForwardingNode<>(nextTab);
        boolean advance = true;
        boolean finishing = false; // to ensure sweep before committing nextTab
        for (int i = 0, bound = 0; ; ) {
            Node<V> f;
            int fh;
            while (advance) {
                int nextIndex, nextBound;
                if (--i >= bound || finishing)
                    advance = false;
                else if ((nextIndex = transferIndex) <= 0) {
                    i = -1;
                    advance = false;
                } else if (Unsafe.getUnsafe().compareAndSwapInt
                        (this, TRANSFERINDEX, nextIndex,
                                nextBound = (nextIndex > stride ?
                                        nextIndex - stride : 0))) {
                    bound = nextBound;
                    i = nextIndex - 1;
                    advance = false;
                }
            }
            if (i < 0 || i >= n || i + n >= nextn) {
                int sc;
                if (finishing) {
                    nextTable = null;
                    table = nextTab;
                    sizeCtl = (n << 1) - (n >>> 1);
                    return;
                }
                if (Unsafe.getUnsafe().compareAndSwapInt(this, SIZECTL, sc = sizeCtl, sc - 1)) {
                    if ((sc - 2) != resizeStamp(n) << RESIZE_STAMP_SHIFT)
                        return;
                    finishing = advance = true;
                    i = n; // recheck before commit
                }
            } else if ((f = tabAt(tab, i)) == null)
                advance = casTabAt(tab, i, fwd);
            else if ((fh = f.hash) == MOVED)
                advance = true; // already processed
            else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        Node<V> ln, hn;
                        if (fh >= 0) {
                            int runBit = fh & n;
                            Node<V> lastRun = f;
                            for (Node<V> p = f.next; p != null; p = p.next) {
                                int b = p.hash & n;
                                if (b != runBit) {
                                    runBit = b;
                                    lastRun = p;
                                }
                            }
                            if (runBit == 0) {
                                ln = lastRun;
                                hn = null;
                            } else {
                                hn = lastRun;
                                ln = null;
                            }
                            for (Node<V> p = f; p != lastRun; p = p.next) {
                                int ph = p.hash;
                                long pk = p.key;
                                V pv = p.val;
                                if ((ph & n) == 0)
                                    ln = new Node<>(ph, pk, pv, ln);
                                else
                                    hn = new Node<>(ph, pk, pv, hn);
                            }
                            setTabAt(nextTab, i, ln);
                            setTabAt(nextTab, i + n, hn);
                            setTabAt(tab, i, fwd);
                            advance = true;
                        } else if (f instanceof TreeBin) {
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> lo = null, loTail = null;
                            TreeNode<V> hi = null, hiTail = null;
                            int lc = 0, hc = 0;
                            for (Node<V> e = t.first; e != null; e = e.next) {
                                int h = e.hash;
                                TreeNode<V> p = new TreeNode<>(h, e.key, e.val, null, null);
                                if ((h & n) == 0) {
                                    if ((p.prev = loTail) == null)
                                        lo = p;
                                    else
                                        loTail.next = p;
                                    loTail = p;
                                    ++lc;
                                } else {
                                    if ((p.prev = hiTail) == null)
                                        hi = p;
                                    else
                                        hiTail.next = p;
                                    hiTail = p;
                                    ++hc;
                                }
                            }
                            ln = (lc <= UNTREEIFY_THRESHOLD) ? untreeify(lo) :
                                    (hc != 0) ? new TreeBin<>(lo) : t;
                            hn = (hc <= UNTREEIFY_THRESHOLD) ? untreeify(hi) :
                                    (lc != 0) ? new TreeBin<>(hi) : t;
                            setTabAt(nextTab, i, ln);
                            setTabAt(nextTab, i + n, hn);
                            setTabAt(tab, i, fwd);
                            advance = true;
                        }
                    }
                }
            }
        }
    }
    /* ---------------- Counter support -------------- */

    /**
     * Replaces all linked nodes in bin at given index unless table is
     * too small, in which case resizes instead.
     */
    private void treeifyBin(Node<V>[] tab, int index) {
        Node<V> b;
        int n, sc;
        if (tab != null) {
            if ((n = tab.length) < MIN_TREEIFY_CAPACITY)
                tryPresize(n << 1);
            else if ((b = tabAt(tab, index)) != null && b.hash >= 0) {
                synchronized (b) {
                    if (tabAt(tab, index) == b) {
                        TreeNode<V> hd = null, tl = null;
                        for (Node<V> e = b; e != null; e = e.next) {
                            TreeNode<V> p =
                                    new TreeNode<>(e.hash, e.key, e.val, null, null);
                            if ((p.prev = tl) == null)
                                hd = p;
                            else
                                tl.next = p;
                            tl = p;
                        }
                        setTabAt(tab, index, new TreeBin<>(hd));
                    }
                }
            }
        }
    }

    /**
     * Tries to presize table to accommodate the given number of elements.
     *
     * @param size number of elements (doesn't need to be perfectly accurate)
     */
    private void tryPresize(int size) {
        int c = (size >= (MAXIMUM_CAPACITY >>> 1)) ? MAXIMUM_CAPACITY :
                tableSizeFor(size + (size >>> 1) + 1);
        int sc;
        while ((sc = sizeCtl) >= 0) {
            Node<V>[] tab = table;
            int n;
            if (tab == null || (n = tab.length) == 0) {
                n = Math.max(sc, c);
                if (Unsafe.getUnsafe().compareAndSwapInt(this, SIZECTL, sc, -1)) {
                    try {
                        if (table == tab) {
                            @SuppressWarnings("unchecked")
                            Node<V>[] nt = (Node<V>[]) new Node<?>[n];
                            table = nt;
                            sc = n - (n >>> 2);
                        }
                    } finally {
                        sizeCtl = sc;
                    }
                }
            } else if (c <= sc || n >= MAXIMUM_CAPACITY)
                break;
            else if (tab == table) {
                int rs = resizeStamp(n);
                if (sc < 0) {
                    Node<V>[] nt;
                    if ((sc >>> RESIZE_STAMP_SHIFT) != rs || sc == rs + 1 ||
                            sc == rs + MAX_RESIZERS || (nt = nextTable) == null ||
                            transferIndex <= 0)
                        break;
                    if (Unsafe.getUnsafe().compareAndSwapInt(this, SIZECTL, sc, sc + 1))
                        transfer(tab, nt);
                } else if (Unsafe.getUnsafe().compareAndSwapInt(this, SIZECTL, sc,
                        (rs << RESIZE_STAMP_SHIFT) + 2))
                    transfer(tab, null);
            }
        }
    }

    static int advanceProbe(int probe) {
        probe ^= probe << 13;   // xorshift
        probe ^= probe >>> 17;
        probe ^= probe << 5;
        Unsafe.getUnsafe().putInt(Thread.currentThread(), PROBE, probe);
        return probe;
    }

    /* ---------------- Conversion from/to TreeBins -------------- */

    static <V> boolean casTabAt(Node<V>[] tab, int i,
                                Node<V> v) {
        return Unsafe.getUnsafe().compareAndSwapObject(tab, ((long) i << ASHIFT) + ABASE, null, v);
    }

    /**
     * Returns x's Class if it is of the form "class C implements
     * Comparable<C>", else null.
     */
    static Class<?> comparableClassFor(Object x) {
        if (x instanceof Comparable) {
            Class<?> c;
            Type[] ts, as;
            Type t;
            ParameterizedType p;
            if ((c = x.getClass()) == String.class) // bypass checks
                return c;
            if ((ts = c.getGenericInterfaces()) != null) {
                for (int i = 0; i < ts.length; ++i) {
                    if (((t = ts[i]) instanceof ParameterizedType) &&
                            ((p = (ParameterizedType) t).getRawType() ==
                                    Comparable.class) &&
                            (as = p.getActualTypeArguments()) != null &&
                            as.length == 1 && as[0] == c) // type arg is c
                        return c;
                }
            }
        }
        return null;
    }

    /* ---------------- TreeNodes -------------- */

    /**
     * Returns k.compareTo(x) if x matches kc (k's screened comparable
     * class), else 0.
     */
    static int compareComparables(long k, long x) {
        return Long.compare(k, x);
    }

    /* ---------------- TreeBins -------------- */

    static int getProbe() {
        return Unsafe.getUnsafe().getInt(Thread.currentThread(), PROBE);
    }

    /* ----------------Table Traversal -------------- */

    /**
     * Initialize Thread fields for the current thread.  Called only
     * when Thread.threadLocalRandomProbe is zero, indicating that a
     * thread local seed value needs to be generated. Note that even
     * though the initialization is purely thread-local, we need to
     * rely on (static) atomic generators to initialize the values.
     */
    static void localInit() {
        int p = probeGenerator.addAndGet(PROBE_INCREMENT);
        int probe = (p == 0) ? 1 : p; // skip 0
        long seed = mix64(seeder.getAndAdd(SEEDER_INCREMENT));
        Thread t = Thread.currentThread();
        Unsafe.getUnsafe().putLong(t, SEED, seed);
        Unsafe.getUnsafe().putInt(t, PROBE, probe);
    }

    /**
     * Returns the stamp bits for resizing a table of size n.
     * Must be negative when shifted left by RESIZE_STAMP_SHIFT.
     */
    static int resizeStamp(int n) {
        return Integer.numberOfLeadingZeros(n) | (1 << (RESIZE_STAMP_BITS - 1));
    }

    static <K, V> void setTabAt(Node<V>[] tab, int i, Node<V> v) {
        Unsafe.getUnsafe().putObjectVolatile(tab, ((long) i << ASHIFT) + ABASE, v);
    }

    /**
     * Spreads (XORs) higher bits of hash to lower and also forces top
     * bit to 0. Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will
     * always collide. (Among known examples are sets of Float keys
     * holding consecutive whole numbers in small tables.)  So we
     * apply a transform that spreads the impact of higher bits
     * downward. There is a tradeoff between speed, utility, and
     * quality of bit-spreading. Because many common sets of hashes
     * are already reasonably distributed (so don't benefit from
     * spreading), and because we use trees to handle large sets of
     * collisions in bins, we just XOR some shifted bits in the
     * cheapest possible way to reduce systematic lossage, as well as
     * to incorporate impact of the highest bits that would otherwise
     * never be used in index calculations because of table bounds.
     */
    static int spread(int h) {
        return (h ^ (h >>> 16)) & HASH_BITS;
    }

    @SuppressWarnings("unchecked")
    static <V> Node<V> tabAt(Node<V>[] tab, int i) {
        return (Node<V>) Unsafe.getUnsafe().getObjectVolatile(tab, ((long) i << ASHIFT) + ABASE);
    }

    /**
     * Returns a list on non-TreeNodes replacing those in given list.
     */
    static <V> Node<V> untreeify(Node<V> b) {
        Node<V> hd = null, tl = null;
        for (Node<V> q = b; q != null; q = q.next) {
            Node<V> p = new Node<>(q.hash, q.key, q.val, null);
            if (tl == null)
                hd = p;
            else
                tl.next = p;
            tl = p;
        }
        return hd;
    }

    /**
     * Helps transfer if a resize is in progress.
     */
    final Node<V>[] helpTransfer(Node<V>[] tab, Node<V> f) {
        Node<V>[] nextTab;
        int sc;
        if (tab != null && (f instanceof ForwardingNode) &&
                (nextTab = ((ForwardingNode<V>) f).nextTable) != null) {
            int rs = resizeStamp(tab.length);
            while (nextTab == nextTable && table == tab &&
                    (sc = sizeCtl) < 0) {
                if ((sc >>> RESIZE_STAMP_SHIFT) != rs || sc == rs + 1 ||
                        sc == rs + MAX_RESIZERS || transferIndex <= 0)
                    break;
                if (Unsafe.getUnsafe().compareAndSwapInt(this, SIZECTL, sc, sc + 1)) {
                    transfer(tab, nextTab);
                    break;
                }
            }
            return nextTab;
        }
        return table;
    }

    /* ----------------Views -------------- */

    /**
     * Implementation for put and putIfAbsent
     */
    final V putVal(long key, V value, boolean onlyIfAbsent) {
        if (key < 0) throw new IllegalArgumentException();
        if (value == null) throw new NullPointerException();
        int hash = spread(keyHashCode(key));
        int binCount = 0;
        Node<V> _new = null;

        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & hash)) == null) {
                if (_new == null) {
                    _new = new Node<>(hash, key, value, null);
                }
                if (casTabAt(tab, i, _new)) {
                    break;                   // no lock when adding to empty bin
                }
            } else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                V oldVal = null;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f; ; ++binCount) {
                                if (e.hash == hash && e.key == key) {
                                    oldVal = e.val;
                                    if (!onlyIfAbsent)
                                        e.val = value;
                                    break;
                                }
                                Node<V> pred = e;
                                if ((e = e.next) == null) {
                                    if (_new == null) {
                                        pred.next = new Node<>(hash, key, value, null);
                                    } else {
                                        pred.next = _new;
                                    }
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            Node<V> p;
                            binCount = 2;
                            if ((p = ((TreeBin<V>) f).putTreeVal(hash, key, value)) != null) {
                                oldVal = p.val;
                                if (!onlyIfAbsent)
                                    p.val = value;
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD)
                        treeifyBin(tab, i);
                    if (oldVal != null)
                        return oldVal;
                    break;
                }
            }
        }
        addCount(1L, binCount);
        return null;
    }

    /**
     * Implementation for the four public remove/replace methods:
     * Replaces node value with v, conditional upon match of cv if
     * non-null.  If resulting value is null, delete.
     */
    final V replaceNode(long key, V value, V cv) {
        int hash = spread(keyHashCode(key));
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0 ||
                    (f = tabAt(tab, i = (n - 1) & hash)) == null)
                break;
            else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                V oldVal = null;
                boolean validated = false;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            validated = true;
                            for (Node<V> e = f, pred = null; ; ) {
                                if (e.hash == hash && e.key == key) {
                                    V ev = e.val;
                                    if (cv == null || cv == ev || (cv.equals(ev))) {
                                        oldVal = ev;
                                        if (value != null)
                                            e.val = value;
                                        else if (pred != null)
                                            pred.next = e.next;
                                        else
                                            setTabAt(tab, i, e.next);
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null)
                                    break;
                            }
                        } else if (f instanceof TreeBin) {
                            validated = true;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r, p;
                            if ((r = t.root) != null &&
                                    (p = r.findTreeNode(hash, key)) != null) {
                                V pv = p.val;
                                if (cv == null || cv == pv || cv.equals(pv)) {
                                    oldVal = pv;
                                    if (value != null)
                                        p.val = value;
                                    else if (t.removeTreeNode(p))
                                        setTabAt(tab, i, untreeify(t.first));
                                }
                            }
                        }
                    }
                }
                if (validated) {
                    if (oldVal != null) {
                        if (value == null)
                            addCount(-1L, -1);
                        return oldVal;
                    }
                    break;
                }
            }
        }
        return null;
    }

    final long sumCount() {
        CounterCell[] as = counterCells;
        CounterCell a;
        long sum = baseCount;
        if (as != null) {
            for (int i = 0; i < as.length; ++i) {
                if ((a = as[i]) != null)
                    sum += a.value;
            }
        }
        return sum;
    }

    public interface LongEntry<V> {
        boolean equals(Object var1);

        long getKey();

        V getValue();

        int hashCode();

        V setValue(V var1);
    }

    /**
     * Base of key, value, and entry Iterators. Adds fields to
     * Traverser to support iterator.remove.
     */
    static class BaseIterator<V> extends Traverser<V> {
        Node<V> lastReturned;
        ConcurrentLongHashMap<V> map;

        public final boolean hasNext() {
            return next != null;
        }

        public final void remove() {
            Node<V> p;
            if ((p = lastReturned) == null)
                throw new IllegalStateException();
            lastReturned = null;
            map.replaceNode(p.key, null, null);
        }

        void of(ConcurrentLongHashMap<V> map) {
            Node<V>[] tab = map.table;
            int l = tab == null ? 0 : tab.length;
            super.of(tab, l, l);
            this.map = map;
            advance();
        }
    }

    /**
     * Base class for views.
     */
    abstract static class CollectionView<V, E>
            implements Collection<E>, java.io.Serializable {
        private static final String oomeMsg = "Required array size too large";
        private static final long serialVersionUID = 7249069246763182397L;
        final ConcurrentLongHashMap<V> map;

        CollectionView(ConcurrentLongHashMap<V> map) {
            this.map = map;
        }

        /**
         * Removes all of the elements from this view, by removing all
         * the mappings from the map backing this view.
         */
        public final void clear() {
            map.clear();
        }

        public abstract boolean contains(Object o);

        public final boolean containsAll(@NotNull Collection<?> c) {
            if (c != this) {
                for (Object e : c) {
                    if (e == null || !contains(e))
                        return false;
                }
            }
            return true;
        }

        /**
         * Returns the map backing this view.
         *
         * @return the map backing this view
         */
        public ConcurrentLongHashMap<V> getMap() {
            return map;
        }

        public final boolean isEmpty() {
            return map.isEmpty();
        }

        /**
         * Returns an iterator over the elements in this collection.
         * <p>The returned iterator is
         * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
         *
         * @return an iterator over the elements in this collection
         */
        @NotNull
        public abstract Iterator<E> iterator();

        public abstract boolean remove(Object o);

        @Override
        public final boolean removeAll(@NotNull Collection<?> c) {
            boolean modified = false;
            for (Iterator<E> it = iterator(); it.hasNext(); ) {
                if (c.contains(it.next())) {
                    it.remove();
                    modified = true;
                }
            }
            return modified;
        }

        // implementations below rely on concrete classes supplying these
        // abstract methods

        @Override
        public final boolean retainAll(@NotNull Collection<?> c) {
            boolean modified = false;
            for (Iterator<E> it = iterator(); it.hasNext(); ) {
                if (!c.contains(it.next())) {
                    it.remove();
                    modified = true;
                }
            }
            return modified;
        }

        @Override
        public final int size() {
            return map.size();
        }

        @Override
        public final Object @NotNull [] toArray() {
            long sz = map.mappingCount();
            if (sz > MAX_ARRAY_SIZE)
                throw new OutOfMemoryError(oomeMsg);
            int n = (int) sz;
            Object[] r = new Object[n];
            int i = 0;
            for (E e : this) {
                if (i == n) {
                    if (n >= MAX_ARRAY_SIZE)
                        throw new OutOfMemoryError(oomeMsg);
                    if (n >= MAX_ARRAY_SIZE - (MAX_ARRAY_SIZE >>> 1) - 1)
                        n = MAX_ARRAY_SIZE;
                    else
                        n += (n >>> 1) + 1;
                    r = Arrays.copyOf(r, n);
                }
                r[i++] = e;
            }
            return (i == n) ? r : Arrays.copyOf(r, i);
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T @NotNull [] toArray(@NotNull T @NotNull [] a) {
            long sz = map.mappingCount();
            if (sz > MAX_ARRAY_SIZE)
                throw new OutOfMemoryError(oomeMsg);
            int m = (int) sz;
            T[] r = (a.length >= m) ? a :
                    (T[]) java.lang.reflect.Array
                            .newInstance(a.getClass().getComponentType(), m);
            int n = r.length;
            int i = 0;
            for (E e : this) {
                if (i == n) {
                    if (n >= MAX_ARRAY_SIZE)
                        throw new OutOfMemoryError(oomeMsg);
                    if (n >= MAX_ARRAY_SIZE - (MAX_ARRAY_SIZE >>> 1) - 1)
                        n = MAX_ARRAY_SIZE;
                    else
                        n += (n >>> 1) + 1;
                    r = Arrays.copyOf(r, n);
                }
                r[i++] = (T) e;
            }
            if (a == r && i < n) {
                r[i] = null; // null-terminate
                return r;
            }
            return (i == n) ? r : Arrays.copyOf(r, i);
        }

        /**
         * Returns a string representation of this collection.
         * The string representation consists of the string representations
         * of the collection's elements in the order they are returned by
         * its iterator, enclosed in square brackets ({@code "[]"}).
         * Adjacent elements are separated by the characters {@code ", "}
         * (comma and space).  Elements are converted to strings as by
         * {@link String#valueOf(Object)}.
         *
         * @return a string representation of this collection
         */
        @Override
        public final String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            Iterator<E> it = iterator();
            if (it.hasNext()) {
                for (; ; ) {
                    Object e = it.next();
                    sb.append(e == this ? "(this Collection)" : e);
                    if (!it.hasNext())
                        break;
                    sb.append(',').append(' ');
                }
            }
            return sb.append(']').toString();
        }
    }

    /**
     * A padded cell for distributing counts.  Adapted from LongAdder
     * and Striped64.  See their internal docs for explanation.
     */
    static final class CounterCell {
        final long value;

        CounterCell(long x) {
            value = x;
        }
    }

    static final class EntryIterator<V> extends BaseIterator<V>
            implements Iterator<LongEntry<V>> {

        @Override
        public LongEntry<V> next() {
            Node<V> p;
            if ((p = next) == null)
                throw new NoSuchElementException();
            long k = p.key;
            V v = p.val;
            lastReturned = p;
            advance();
            return new MapEntry<>(k, v, map);
        }
    }

    /**
     * A view of a ConcurrentLongHashMap as a {@link Set} of (key, value)
     * entries.  This class cannot be directly instantiated. See
     * {@link #entrySet()}.
     */
    static final class EntrySetView<V> extends CollectionView<V, LongEntry<V>>
            implements Set<LongEntry<V>>, java.io.Serializable {
        private static final long serialVersionUID = 2249069246763182397L;

        private final ThreadLocal<EntryIterator<V>> tlEntryIterator = ThreadLocal.withInitial(EntryIterator::new);

        EntrySetView(ConcurrentLongHashMap<V> map) {
            super(map);
        }

        @Override
        public boolean add(LongEntry<V> e) {
            return map.putVal(e.getKey(), e.getValue(), false) == null;
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends LongEntry<V>> c) {
            boolean added = false;
            for (LongEntry<V> e : c) {
                if (add(e))
                    added = true;
            }
            return added;
        }

        @Override
        public boolean contains(Object o) {
            long k;
            Object v, r;
            LongEntry<?> e;
            return ((o instanceof LongEntry) &&
                    (k = (e = (LongEntry<?>) o).getKey()) != EMPTY_KEY &&
                    (r = map.get(k)) != null &&
                    (v = e.getValue()) != null &&
                    (v == r || v.equals(r)));
        }

        @Override
        public boolean equals(Object o) {
            Set<?> c;
            return ((o instanceof Set) &&
                    ((c = (Set<?>) o) == this ||
                            (containsAll(c) && c.containsAll(this))));
        }

        @Override
        public int hashCode() {
            int h = 0;
            Node<V>[] t = map.table;
            if (t != null) {
                Traverser<V> it = map.getTraverser(t);
                for (Node<V> p; (p = it.advance()) != null; ) {
                    h += p.hashCode();
                }
            }
            return h;
        }

        /**
         * @return an iterator over the entries of the backing map
         */
        @NotNull
        public Iterator<LongEntry<V>> iterator() {
            EntryIterator<V> it = tlEntryIterator.get();
            it.of(map);
            return it;
        }

        @Override
        public boolean remove(Object o) {
            long k;
            Object v;
            LongEntry<?> e;
            return ((o instanceof LongEntry) &&
                    (k = (e = (LongEntry<?>) o).getKey()) != EMPTY_KEY &&
                    (v = e.getValue()) != null &&
                    map.remove(k, (V) v));
        }
    }

    /**
     * A node inserted at head of bins during transfer operations.
     */
    static final class ForwardingNode<V> extends Node<V> {
        final Node<V>[] nextTable;

        ForwardingNode(Node<V>[] tab) {
            super(MOVED, EMPTY_KEY, null, null);
            this.nextTable = tab;
        }

        @Override
        Node<V> find(int h, long k) {
            // loop to avoid arbitrarily deep recursion on forwarding nodes
            outer:
            for (Node<V>[] tab = nextTable; ; ) {
                Node<V> e;
                int n;
                if (k == EMPTY_KEY || tab == null || (n = tab.length) == 0 ||
                        (e = tabAt(tab, (n - 1) & h)) == null)
                    return null;
                for (; ; ) {
                    int eh;
                    if ((eh = e.hash) == h && e.key == k)
                        return e;
                    if (eh < 0) {
                        if (e instanceof ForwardingNode) {
                            tab = ((ForwardingNode<V>) e).nextTable;
                            continue outer;
                        } else
                            return e.find(h, k);
                    }
                    if ((e = e.next) == null)
                        return null;
                }
            }
        }
    }

    public static final class KeyIterator<V> extends BaseIterator<V> {

        public long next() {
            Node<V> p;
            if ((p = next) == null)
                throw new NoSuchElementException();
            long k = p.key;
            lastReturned = p;
            advance();
            return k;
        }
    }

    /**
     * A view of a ConcurrentLongHashMap as a long set of keys, in
     * which additions may optionally be enabled by mapping to a
     * common value.  This class cannot be directly instantiated.
     * See {@link #keySet() keySet()},
     * {@link #keySet(Object) keySet(V)},
     * {@link #newKeySet() newKeySet()},
     * {@link #newKeySet(int) newKeySet(int)}.
     *
     * @since 1.8
     */
    public static class KeySetView<V> implements java.io.Serializable {
        private static final long serialVersionUID = 7249069246763182397L;
        private final ConcurrentLongHashMap<V> map;
        private final ThreadLocal<KeyIterator<V>> tlKeyIterator = ThreadLocal.withInitial(KeyIterator::new);
        private final V value;

        KeySetView(ConcurrentLongHashMap<V> map, V value) {  // non-public
            this.map = map;
            this.value = value;
        }

        /**
         * Adds the specified key to this set view by mapping the key to
         * the default mapped value in the backing map, if defined.
         *
         * @param k key to be added
         * @return {@code true} if this set changed as a result of the call
         * @throws NullPointerException          if the specified key is null
         * @throws UnsupportedOperationException if no default mapped value
         *                                       for additions was provided
         */
        public boolean add(long k) {
            V v;
            if ((v = value) == null)
                throw new UnsupportedOperationException();
            return map.putVal(k, v, true) == null;
        }

        public final void clear() {
            map.clear();
        }

        public boolean contains(long k) {
            return map.containsKey(k);
        }

        @Override
        public boolean equals(Object o) {
            KeySetView<?> c;
            return ((o instanceof KeySetView) &&
                    ((c = (KeySetView<?>) o) == this ||
                            (containsAll(c) && c.containsAll(this))));
        }

        /**
         * Returns the default mapped value for additions,
         * or {@code null} if additions are not supported.
         *
         * @return the default mapped value for additions, or {@code null}
         * if not supported
         */
        public V getMappedValue() {
            return value;
        }

        @Override
        public int hashCode() {
            int h = 0;
            KeyIterator<V> it = iterator();
            if (it.hasNext()) {
                do {
                    long k = it.next();
                    h += keyHashCode(k);
                } while (it.hasNext());
            }
            return h;
        }

        public final boolean isEmpty() {
            return map.isEmpty();
        }

        /**
         * @return an iterator over the keys of the backing map
         */
        @NotNull
        public KeyIterator<V> iterator() {
            KeyIterator<V> it = tlKeyIterator.get();
            it.of(map);
            return it;
        }

        /**
         * Removes the key from this map view, by removing the key (and its
         * corresponding value) from the backing map.  This method does
         * nothing if the key is not in the map.
         *
         * @param k the key to be removed from the backing map
         * @return {@code true} if the backing map contained the specified key
         * @throws NullPointerException if the specified key is null
         */
        public boolean remove(long k) {
            return map.remove(k) != null;
        }

        public final int size() {
            return map.size();
        }

        @Override
        public final String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            KeyIterator<V> it = iterator();
            if (it.hasNext()) {
                for (; ; ) {
                    long k = it.next();
                    sb.append(k);
                    if (!it.hasNext())
                        break;
                    sb.append(',').append(' ');
                }
            }
            return sb.append(']').toString();
        }

        private boolean containsAll(@NotNull KeySetView<?> c) {
            KeyIterator<V> it = iterator();
            if (it.hasNext()) {
                do {
                    long k = it.next();
                    if (!contains(k))
                        return false;
                } while (it.hasNext());
            }
            return true;
        }
    }

    /**
     * Exported Entry for EntryIterator
     */
    static final class MapEntry<V> implements LongEntry<V> {
        final long key; // != EMPTY_KEY
        final ConcurrentLongHashMap<V> map;
        V val;       // non-null

        MapEntry(long key, V val, ConcurrentLongHashMap<V> map) {
            this.key = key;
            this.val = val;
            this.map = map;
        }

        @Override
        public boolean equals(Object o) {
            long k;
            Object v;
            LongEntry<?> e;
            return ((o instanceof LongEntry) &&
                    (k = (e = (LongEntry<?>) o).getKey()) != EMPTY_KEY &&
                    (v = e.getValue()) != null &&
                    (k == key) &&
                    (v == val || v.equals(val)));
        }

        @Override
        public long getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return val;
        }

        @Override
        public int hashCode() {
            return keyHashCode(key) ^ val.hashCode();
        }

        /**
         * Sets our entry's value and writes through to the map. The
         * value to return is somewhat arbitrary here. Since we do not
         * necessarily track asynchronous changes, the most recent
         * "previous" value could be different from what we return (or
         * could even have been removed, in which case the put will
         * re-establish). We do not and cannot guarantee more.
         */
        @NotNull
        public V setValue(V value) {
            if (value == null) throw new NullPointerException();
            V v = val;
            val = value;
            map.put(key, value);
            return v;
        }

        @Override
        public String toString() {
            return key + "=" + val;
        }
    }

    /**
     * Key-value entry.  This class is never exported out as a
     * user-mutable Map.Entry (i.e., one supporting setValue; see
     * MapEntry below), but can be used for read-only traversals used
     * in bulk tasks.  Subclasses of Node with a negative hash field
     * are special, and contain null keys and values (but are never
     * exported).  Otherwise, keys and vals are never null.
     */
    static class Node<V> implements LongEntry<V> {
        final int hash;
        final long key;
        volatile Node<V> next;
        volatile V val;

        Node(int hash, long key, V val, Node<V> next) {
            this.hash = hash;
            this.key = key;
            this.val = val;
            this.next = next;
        }

        @Override
        public final boolean equals(Object o) {
            long k;
            Object v, u;
            LongEntry<?> e;
            return ((o instanceof LongEntry) &&
                    (k = (e = (LongEntry<?>) o).getKey()) != EMPTY_KEY &&
                    (v = e.getValue()) != null &&
                    (k == key) &&
                    (v == (u = val) || v.equals(u)));
        }

        @Override
        public final long getKey() {
            return key;
        }

        @Override
        public final V getValue() {
            return val;
        }

        @Override
        public final int hashCode() {
            return keyHashCode(key) ^ val.hashCode();
        }

        @Override
        public final V setValue(V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final String toString() {
            return key + "=" + val;
        }

        /**
         * Virtualized support for map.get(); overridden in subclasses.
         */
        Node<V> find(int h, long k) {
            Node<V> e = this;
            if (k != EMPTY_KEY) {
                do {
                    if (e.hash == h && (e.key == k))
                        return e;
                } while ((e = e.next) != null);
            }
            return null;
        }
    }

    /**
     * A place-holder node used in computeIfAbsent and compute
     */
    static final class ReservationNode<V> extends Node<V> {
        ReservationNode() {
            super(RESERVED, EMPTY_KEY, null, null);
        }

        @Override
        Node<V> find(int h, long k) {
            return null;
        }
    }

    /**
     * Stripped-down version of helper class used in previous version,
     * declared for the sake of serialization compatibility
     */
    static class Segment extends ReentrantLock implements Serializable {
        private static final long serialVersionUID = 2249069246763182397L;
        final float loadFactor;

        Segment() {
            this.loadFactor = ConcurrentLongHashMap.LOAD_FACTOR;
        }
    }

    /**
     * Records the table, its length, and current traversal index for a
     * traverser that must process a region of a forwarded table before
     * proceeding with current table.
     */
    static final class TableStack<V> {
        int index;
        int length;
        TableStack<V> next;
        Node<V>[] tab;
    }

    /**
     * Encapsulates traversal for methods such as containsValue; also
     * serves as a base class for other iterators and spliterators.
     * <p>
     * Method advance visits once each still-valid node that was
     * reachable upon iterator construction. It might miss some that
     * were added to a bin after the bin was visited, which is OK wrt
     * consistency guarantees. Maintaining this property in the face
     * of possible ongoing resizes requires a fair amount of
     * bookkeeping state that is difficult to optimize away amidst
     * volatile accesses.  Even so, traversal maintains reasonable
     * throughput.
     * <p>
     * Normally, iteration proceeds bin-by-bin traversing lists.
     * However, if the table has been resized, then all future steps
     * must traverse both the bin at the current index as well as at
     * (index + baseSize); and so on for further resizings. To
     * paranoically cope with potential sharing by users of iterators
     * across threads, iteration terminates if a bounds checks fails
     * for a table read.
     */
    static class Traverser<V> {
        int baseIndex;          // current index of initial table
        int baseLimit;          // index bound for initial table
        int baseSize;     // initial table size
        int index;              // index of bin to use next
        Node<V> next;         // the next entry to use
        TableStack<V> stack, spare; // to save/restore on ForwardingNodes
        Node<V>[] tab;        // current table; updated if resized

        /**
         * Saves traversal state upon encountering a forwarding node.
         */
        private void pushState(Node<V>[] t, int i, int n) {
            TableStack<V> s = spare;  // reuse if possible
            if (s != null)
                spare = s.next;
            else
                s = new TableStack<>();
            s.tab = t;
            s.length = n;
            s.index = i;
            s.next = stack;
            stack = s;
        }

        /**
         * Possibly pops traversal state.
         *
         * @param n length of current table
         */
        private void recoverState(int n) {
            TableStack<V> s;
            int len;
            while ((s = stack) != null && (index += (len = s.length)) >= n) {
                n = len;
                index = s.index;
                tab = s.tab;
                s.tab = null;
                TableStack<V> next = s.next;
                s.next = spare; // save for reuse
                stack = next;
                spare = s;
            }
            if (s == null && (index += baseSize) >= n)
                index = ++baseIndex;
        }

        /**
         * Advances if possible, returning next valid node, or null if none.
         */
        final Node<V> advance() {
            Node<V> e;
            if ((e = next) != null)
                e = e.next;
            for (; ; ) {
                Node<V>[] t;
                int i, n;  // must use locals in checks
                if (e != null)
                    return next = e;
                if (baseIndex >= baseLimit || (t = tab) == null ||
                        (n = t.length) <= (i = index) || i < 0)
                    return next = null;
                if ((e = tabAt(t, i)) != null && e.hash < 0) {
                    if (e instanceof ForwardingNode) {
                        tab = ((ForwardingNode<V>) e).nextTable;
                        e = null;
                        pushState(t, i, n);
                        continue;
                    } else if (e instanceof TreeBin)
                        e = ((TreeBin<V>) e).first;
                    else
                        e = null;
                }
                if (stack != null)
                    recoverState(n);
                else if ((index = i + baseSize) >= n)
                    index = ++baseIndex; // visit upper slots if present
            }
        }

        void of(Node<V>[] tab, int size, int limit) {
            this.tab = tab;
            this.baseSize = size;
            this.baseIndex = this.index = 0;
            this.baseLimit = limit;
            this.next = null;
        }
    }

    /**
     * TreeNodes used at the heads of bins. TreeBins do not hold user
     * keys or values, but instead point to list of TreeNodes and
     * their root. They also maintain a parasitic read-write lock
     * forcing writers (who hold bin lock) to wait for readers (who do
     * not) to complete before tree restructuring operations.
     */
    static final class TreeBin<V> extends Node<V> {
        static final int READER = 4; // increment value for setting read lock
        static final int WAITER = 2; // set when waiting for write lock
        // values for lockState
        static final int WRITER = 1; // set while holding write lock
        private static final long LOCKSTATE;
        private static final sun.misc.Unsafe U;
        volatile TreeNode<V> first;
        volatile int lockState;
        TreeNode<V> root;
        volatile Thread waiter;

        /**
         * Creates bin with initial set of nodes headed by b.
         */
        TreeBin(TreeNode<V> b) {
            super(TREEBIN, EMPTY_KEY, null, null);
            this.first = b;
            TreeNode<V> r = null;
            for (TreeNode<V> x = b, next; x != null; x = next) {
                next = (TreeNode<V>) x.next;
                x.left = x.right = null;
                if (r == null) {
                    x.parent = null;
                    x.red = false;
                    r = x;
                } else {
                    long k = x.key;
                    int h = x.hash;
                    for (TreeNode<V> p = r; ; ) {
                        int dir, ph;
                        long pk = p.key;
                        if ((ph = p.hash) > h)
                            dir = -1;
                        else if (ph < h)
                            dir = 1;
                        else if ((dir = compareComparables(k, pk)) == 0)
                            dir = tieBreakOrder(k, pk);
                        TreeNode<V> xp = p;
                        if ((p = (dir <= 0) ? p.left : p.right) == null) {
                            x.parent = xp;
                            if (dir <= 0)
                                xp.left = x;
                            else
                                xp.right = x;
                            r = balanceInsertion(r, x);
                            break;
                        }
                    }
                }
            }
            this.root = r;
            assert checkInvariants(root);
        }

        /**
         * Possibly blocks awaiting root lock.
         */
        private void contendedLock() {
            boolean waiting = false;
            for (int s; ; ) {
                if (((s = lockState) & ~WAITER) == 0) {
                    if (U.compareAndSwapInt(this, LOCKSTATE, s, WRITER)) {
                        if (waiting)
                            waiter = null;
                        return;
                    }
                } else if ((s & WAITER) == 0) {
                    if (U.compareAndSwapInt(this, LOCKSTATE, s, s | WAITER)) {
                        waiting = true;
                        waiter = Thread.currentThread();
                    }
                } else if (waiting)
                    LockSupport.park(this);
            }
        }

        /**
         * Acquires write lock for tree restructuring.
         */
        private void lockRoot() {
            if (!U.compareAndSwapInt(this, LOCKSTATE, 0, WRITER))
                contendedLock(); // offload to separate method
        }

        /**
         * Releases write lock for tree restructuring.
         */
        private void unlockRoot() {
            lockState = 0;
        }

        static <V> TreeNode<V> balanceDeletion(TreeNode<V> root,
                                               TreeNode<V> x) {
            for (TreeNode<V> xp, xpl, xpr; ; ) {
                if (x == null || x == root)
                    return root;
                else if ((xp = x.parent) == null) {
                    x.red = false;
                    return x;
                } else if (x.red) {
                    x.red = false;
                    return root;
                } else if ((xpl = xp.left) == x) {
                    if ((xpr = xp.right) != null && xpr.red) {
                        xpr.red = false;
                        xp.red = true;
                        root = rotateLeft(root, xp);
                        xpr = (xp = x.parent) == null ? null : xp.right;
                    }
                    if (xpr == null)
                        x = xp;
                    else {
                        TreeNode<V> sl = xpr.left, sr = xpr.right;
                        if ((sr == null || !sr.red) &&
                                (sl == null || !sl.red)) {
                            xpr.red = true;
                            x = xp;
                        } else {
                            if (sr == null || !sr.red) {
                                sl.red = false;
                                xpr.red = true;
                                root = rotateRight(root, xpr);
                                xpr = (xp = x.parent) == null ?
                                        null : xp.right;
                            }
                            if (xpr != null) {
                                xpr.red = xp.red;
                                if ((sr = xpr.right) != null)
                                    sr.red = false;
                            }
                            if (xp != null) {
                                xp.red = false;
                                root = rotateLeft(root, xp);
                            }
                            x = root;
                        }
                    }
                } else { // symmetric
                    if (xpl != null && xpl.red) {
                        xpl.red = false;
                        xp.red = true;
                        root = rotateRight(root, xp);
                        xpl = (xp = x.parent) == null ? null : xp.left;
                    }
                    if (xpl == null)
                        x = xp;
                    else {
                        TreeNode<V> sl = xpl.left, sr = xpl.right;
                        if ((sl == null || !sl.red) &&
                                (sr == null || !sr.red)) {
                            xpl.red = true;
                            x = xp;
                        } else {
                            if (sl == null || !sl.red) {
                                sr.red = false;
                                xpl.red = true;
                                root = rotateLeft(root, xpl);
                                xpl = (xp = x.parent) == null ?
                                        null : xp.left;
                            }
                            if (xpl != null) {
                                xpl.red = xp.red;
                                if ((sl = xpl.left) != null)
                                    sl.red = false;
                            }
                            if (xp != null) {
                                xp.red = false;
                                root = rotateRight(root, xp);
                            }
                            x = root;
                        }
                    }
                }
            }
        }

        static <V> TreeNode<V> balanceInsertion(TreeNode<V> root,
                                                TreeNode<V> x) {
            x.red = true;
            for (TreeNode<V> xp, xpp, xppl, xppr; ; ) {
                if ((xp = x.parent) == null) {
                    x.red = false;
                    return x;
                } else if (!xp.red || (xpp = xp.parent) == null)
                    return root;
                if (xp == (xppl = xpp.left)) {
                    if ((xppr = xpp.right) != null && xppr.red) {
                        xppr.red = false;
                        xp.red = false;
                        xpp.red = true;
                        x = xpp;
                    } else {
                        if (x == xp.right) {
                            root = rotateLeft(root, x = xp);
                            xpp = (xp = x.parent) == null ? null : xp.parent;
                        }
                        if (xp != null) {
                            xp.red = false;
                            if (xpp != null) {
                                xpp.red = true;
                                root = rotateRight(root, xpp);
                            }
                        }
                    }
                } else {
                    if (xppl != null && xppl.red) {
                        xppl.red = false;
                        xp.red = false;
                        xpp.red = true;
                        x = xpp;
                    } else {
                        if (x == xp.left) {
                            root = rotateRight(root, x = xp);
                            xpp = (xp = x.parent) == null ? null : xp.parent;
                        }
                        if (xp != null) {
                            xp.red = false;
                            if (xpp != null) {
                                xpp.red = true;
                                root = rotateLeft(root, xpp);
                            }
                        }
                    }
                }
            }
        }

        /**
         * Recursive invariant check
         */
        @SuppressWarnings("SimplifiableIfStatement")
        static <V> boolean checkInvariants(TreeNode<V> t) {
            TreeNode<V> tp = t.parent, tl = t.left, tr = t.right,
                    tb = t.prev, tn = (TreeNode<V>) t.next;
            if (tb != null && tb.next != t)
                return false;
            if (tn != null && tn.prev != t)
                return false;
            if (tp != null && t != tp.left && t != tp.right)
                return false;
            if (tl != null && (tl.parent != t || tl.hash > t.hash))
                return false;
            if (tr != null && (tr.parent != t || tr.hash < t.hash))
                return false;
            if (t.red && tl != null && tl.red && tr != null && tr.red)
                return false;
            if (tl != null && !checkInvariants(tl))
                return false;
            return !(tr != null && !checkInvariants(tr));
        }

        static <V> TreeNode<V> rotateLeft(TreeNode<V> root, TreeNode<V> p) {
            TreeNode<V> r, pp, rl;
            if (p != null && (r = p.right) != null) {
                if ((rl = p.right = r.left) != null)
                    rl.parent = p;
                if ((pp = r.parent = p.parent) == null)
                    (root = r).red = false;
                else if (pp.left == p)
                    pp.left = r;
                else
                    pp.right = r;
                r.left = p;
                p.parent = r;
            }
            return root;
        }

        static <V> TreeNode<V> rotateRight(TreeNode<V> root, TreeNode<V> p) {
            TreeNode<V> l, pp, lr;
            if (p != null && (l = p.left) != null) {
                if ((lr = p.left = l.right) != null)
                    lr.parent = p;
                if ((pp = l.parent = p.parent) == null)
                    (root = l).red = false;
                else if (pp.right == p)
                    pp.right = l;
                else
                    pp.left = l;
                l.right = p;
                p.parent = l;
            }
            return root;
        }

        /**
         * Tie-breaking utility for ordering insertions when equal
         * hashCodes and non-comparable. We don't require a total
         * order, just a consistent insertion rule to maintain
         * equivalence across rebalancings. Tie-breaking further than
         * necessary simplifies testing a bit.
         */
        static int tieBreakOrder(Object a, Object b) {
            int d;
            if (a == null || b == null ||
                    (d = a.getClass().getName().
                            compareTo(b.getClass().getName())) == 0)
                d = (System.identityHashCode(a) <= System.identityHashCode(b) ?
                        -1 : 1);
            return d;
        }

        /**
         * Returns matching node or null if none. Tries to search
         * using tree comparisons from root, but continues linear
         * search when lock not available.
         */
        @Override
        Node<V> find(int h, long k) {
            if (k != EMPTY_KEY) {
                for (Node<V> e = first; e != null; ) {
                    int s;
                    if (((s = lockState) & (WAITER | WRITER)) != 0) {
                        if (e.hash == h && e.key == k)
                            return e;
                        e = e.next;
                    } else if (U.compareAndSwapInt(this, LOCKSTATE, s,
                            s + READER)) {
                        TreeNode<V> r, p;
                        try {
                            p = ((r = root) == null ? null :
                                    r.findTreeNode(h, k));
                        } finally {
                            Thread w;
                            if (U.getAndAddInt(this, LOCKSTATE, -READER) ==
                                    (READER | WAITER) && (w = waiter) != null)
                                LockSupport.unpark(w);
                        }
                        return p;
                    }
                }
            }
            return null;
        }

        /**
         * Finds or adds a node.
         *
         * @return null if added
         */
        TreeNode<V> putTreeVal(int h, long k, V v) {
            boolean searched = false;
            for (TreeNode<V> p = root; ; ) {
                int dir, ph;
                long pk;
                if (p == null) {
                    first = root = new TreeNode<>(h, k, v, null, null);
                    break;
                } else if ((ph = p.hash) > h)
                    dir = -1;
                else if (ph < h)
                    dir = 1;
                else if ((pk = p.key) == k)
                    return p;
                else if ((dir = compareComparables(k, pk)) == 0) {
                    if (!searched) {
                        TreeNode<V> q, ch;
                        searched = true;
                        if (((ch = p.left) != null &&
                                (q = ch.findTreeNode(h, k)) != null) ||
                                ((ch = p.right) != null &&
                                        (q = ch.findTreeNode(h, k)) != null))
                            return q;
                    }
                    dir = tieBreakOrder(k, pk);
                }

                TreeNode<V> xp = p;
                if ((p = (dir <= 0) ? p.left : p.right) == null) {
                    TreeNode<V> x, f = first;
                    first = x = new TreeNode<>(h, k, v, f, xp);
                    if (f != null)
                        f.prev = x;
                    if (dir <= 0)
                        xp.left = x;
                    else
                        xp.right = x;
                    if (!xp.red)
                        x.red = true;
                    else {
                        lockRoot();
                        try {
                            root = balanceInsertion(root, x);
                        } finally {
                            unlockRoot();
                        }
                    }
                    break;
                }
            }
            assert checkInvariants(root);
            return null;
        }

        /**
         * Removes the given node, that must be present before this
         * call.  This is messier than typical red-black deletion code
         * because we cannot swap the contents of an interior node
         * with a leaf successor that is pinned by "next" pointers
         * that are accessible independently of lock. So instead we
         * swap the tree linkages.
         *
         * @return true if now too small, so should be untreeified
         */
        boolean removeTreeNode(TreeNode<V> p) {
            TreeNode<V> next = (TreeNode<V>) p.next;
            TreeNode<V> pred = p.prev;  // unlink traversal pointers
            TreeNode<V> r, rl;
            if (pred == null)
                first = next;
            else
                pred.next = next;
            if (next != null)
                next.prev = pred;
            if (first == null) {
                root = null;
                return true;
            }
            if ((r = root) == null || r.right == null || // too small
                    (rl = r.left) == null || rl.left == null)
                return true;
            lockRoot();
            try {
                TreeNode<V> replacement;
                TreeNode<V> pl = p.left;
                TreeNode<V> pr = p.right;
                if (pl != null && pr != null) {
                    TreeNode<V> s = pr, sl;
                    while ((sl = s.left) != null) // find successor
                        s = sl;
                    boolean c = s.red;
                    s.red = p.red;
                    p.red = c; // swap colors
                    TreeNode<V> sr = s.right;
                    TreeNode<V> pp = p.parent;
                    if (s == pr) { // p was s's direct parent
                        p.parent = s;
                        s.right = p;
                    } else {
                        TreeNode<V> sp = s.parent;
                        if ((p.parent = sp) != null) {
                            if (s == sp.left)
                                sp.left = p;
                            else
                                sp.right = p;
                        }
                        s.right = pr;
                        pr.parent = s;
                    }
                    p.left = null;
                    if ((p.right = sr) != null)
                        sr.parent = p;
                    s.left = pl;
                    pl.parent = s;
                    if ((s.parent = pp) == null)
                        r = s;
                    else if (p == pp.left)
                        pp.left = s;
                    else
                        pp.right = s;
                    if (sr != null)
                        replacement = sr;
                    else
                        replacement = p;
                } else if (pl != null)
                    replacement = pl;
                else if (pr != null)
                    replacement = pr;
                else
                    replacement = p;
                if (replacement != p) {
                    TreeNode<V> pp = replacement.parent = p.parent;
                    if (pp == null)
                        r = replacement;
                    else if (p == pp.left)
                        pp.left = replacement;
                    else
                        pp.right = replacement;
                    p.left = p.right = p.parent = null;
                }

                root = (p.red) ? r : balanceDeletion(r, replacement);

                if (p == replacement) {  // detach pointers
                    TreeNode<V> pp;
                    if ((pp = p.parent) != null) {
                        if (p == pp.left)
                            pp.left = null;
                        else if (p == pp.right)
                            pp.right = null;
                        p.parent = null;
                    }
                }
            } finally {
                unlockRoot();
            }
            assert checkInvariants(root);
            return false;
        }

        static {
            try {
                U = Unsafe.getUnsafe();
                Class<?> k = TreeBin.class;
                LOCKSTATE = U.objectFieldOffset
                        (k.getDeclaredField("lockState"));
            } catch (Exception e) {
                throw new Error(e);
            }
        }















        /* ------------------------------------------------------------ */
        // Red-black tree methods, all adapted from CLR
    }

    /**
     * Nodes for use in TreeBins
     */
    static final class TreeNode<V> extends Node<V> {
        TreeNode<V> left;
        TreeNode<V> parent;  // red-black tree links
        TreeNode<V> prev;    // needed to unlink next upon deletion
        boolean red;
        TreeNode<V> right;

        TreeNode(int hash, long key, V val, Node<V> next, TreeNode<V> parent) {
            super(hash, key, val, next);
            this.parent = parent;
        }

        @Override
        Node<V> find(int h, long k) {
            return findTreeNode(h, k);
        }

        /**
         * Returns the TreeNode (or null if not found) for the given key
         * starting at given root.
         */
        TreeNode<V> findTreeNode(int h, long k) {
            if (k != EMPTY_KEY) {
                TreeNode<V> p = this;
                do {
                    int ph, dir;
                    long pk;
                    TreeNode<V> q;
                    TreeNode<V> pl = p.left, pr = p.right;
                    if ((ph = p.hash) > h)
                        p = pl;
                    else if (ph < h)
                        p = pr;
                    else if ((pk = p.key) == k)
                        return p;
                    else if (pl == null)
                        p = pr;
                    else if (pr == null)
                        p = pl;
                    else if ((dir = compareComparables(k, pk)) != 0)
                        p = (dir < 0) ? pl : pr;
                    else if ((q = pr.findTreeNode(h, k)) != null)
                        return q;
                    else
                        p = pl;
                } while (p != null);
            }
            return null;
        }


    }

    static final class ValueIterator<V> extends BaseIterator<V>
            implements Iterator<V> {
        public V next() {
            Node<V> p;
            if ((p = next) == null)
                throw new NoSuchElementException();
            V v = p.val;
            lastReturned = p;
            advance();
            return v;
        }
    }

    /**
     * A view of a ConcurrentLongHashMap as a {@link Collection} of
     * values, in which additions are disabled. This class cannot be
     * directly instantiated. See {@link #values()}.
     */
    static final class ValuesView<V> extends CollectionView<V, V>
            implements Collection<V>, java.io.Serializable {
        private static final long serialVersionUID = 2249069246763182397L;
        private final ThreadLocal<ValueIterator<V>> tlValueIterator = ThreadLocal.withInitial(ValueIterator::new);

        ValuesView(ConcurrentLongHashMap<V> map) {
            super(map);
        }

        @Override
        public boolean add(V e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends V> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(Object o) {
            return map.containsValue((V) o);
        }

        @Override
        @NotNull
        public Iterator<V> iterator() {
            ValueIterator<V> it = tlValueIterator.get();
            it.of(map);
            return it;
        }

        @Override
        public boolean remove(Object o) {
            if (o != null) {
                for (Iterator<V> it = iterator(); it.hasNext(); ) {
                    if (o.equals(it.next())) {
                        it.remove();
                        return true;
                    }
                }
            }
            return false;
        }
    }

    static {
        try {
            Class<?> tk = Thread.class;
            SEED = Unsafe.getUnsafe().objectFieldOffset(tk.getDeclaredField("threadLocalRandomSeed"));
            PROBE = Unsafe.getUnsafe().objectFieldOffset(tk.getDeclaredField("threadLocalRandomProbe"));
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    static {
        try {
            Class<?> k = ConcurrentLongHashMap.class;
            SIZECTL = Unsafe.getUnsafe().objectFieldOffset
                    (k.getDeclaredField("sizeCtl"));
            TRANSFERINDEX = Unsafe.getUnsafe().objectFieldOffset
                    (k.getDeclaredField("transferIndex"));
            BASECOUNT = Unsafe.getUnsafe().objectFieldOffset
                    (k.getDeclaredField("baseCount"));
            CELLSBUSY = Unsafe.getUnsafe().objectFieldOffset
                    (k.getDeclaredField("cellsBusy"));
            Class<?> ck = CounterCell.class;
            CELLVALUE = Unsafe.getUnsafe().objectFieldOffset
                    (ck.getDeclaredField("value"));
            Class<?> ak = Node[].class;
            ABASE = Unsafe.getUnsafe().arrayBaseOffset(ak);
            int scale = Unsafe.getUnsafe().arrayIndexScale(ak);
            if ((scale & (scale - 1)) != 0)
                throw new Error("data type scale not a power of two");
            ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
