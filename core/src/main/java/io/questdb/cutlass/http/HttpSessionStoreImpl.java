package io.questdb.cutlass.http;

import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.PrincipalContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.Iterator;
import java.util.Map;

import static io.questdb.cutlass.http.HttpConstants.SESSION_ID_PREFIX;

/**
 * Session store with automatic session ID rotation.
 *
 * <h2>Session ID vs SessionInfo</h2>
 * <ul>
 *   <li><b>SessionInfo:</b> The actual session object containing user principal, groups, expiry, etc.
 *       There is exactly one SessionInfo per active session.</li>
 *   <li><b>Session ID:</b> A string token used to look up the SessionInfo in the map.
 *       Multiple session IDs can point to the same SessionInfo (during rotation).</li>
 * </ul>
 *
 * <h2>How Rotation Works</h2>
 * <p>When a session is rotated:</p>
 * <ol>
 *   <li>Generate a new session ID (e.g., "qs1_new")</li>
 *   <li>Update {@code SessionInfo.sessionId = "qs1_new"}</li>
 *   <li>Add new map entry: {@code map.put("qs1_new", sessionInfo)}</li>
 *   <li><b>The old ID "qs1_old" remains in the map, pointing to the same SessionInfo</b></li>
 * </ol>
 *
 * <p>Result: Both "qs1_old" and "qs1_new" keys exist in the map, both pointing to the same SessionInfo object.
 * The old key can be detected by comparing: {@code mapKey != sessionInfo.getSessionId()}.
 *
 * <h2>Grace Period and Eviction</h2>
 * <p>Old session IDs remain valid for {@code rotatedSessionEvictionTime} after rotation to allow
 * clients with in-flight requests to transition smoothly. After the grace period, old IDs are
 * removed from the map during periodic eviction sweeps.
 *
 * <p><b>Note:</b> {@code SessionInfo.rotateAt} stores when the <i>next</i> rotation should occur,
 * not when the last rotation happened. To calculate when to evict old IDs, the code works backwards:
 * {@code nextRotationTime - gracePeriod}.
 */
public class HttpSessionStoreImpl implements HttpSessionStore {
    private static final Log LOG = LogFactory.getLog(HttpSessionStoreImpl.class);
    private static final int MAX_GENERATION_ATTEMPTS = 5;
    private static final int SESSION_ID_SIZE_BYTES = 32;
    protected final ConcurrentHashMap<ReadOnlyObjList<CharSequence>> groupsByEntity = new ConcurrentHashMap<>();
    private final long evictionCheckInterval;
    private final MicrosecondClock microsClock;
    private final long rotatedSessionEvictionTime;
    private final long rotationPeriod;
    private final long sessionTimeout;
    private final ConcurrentHashMap<SessionInfo> sessionsById = new ConcurrentHashMap<>();
    private volatile long nextEvictionCheckAt;
    private TokenGenerator tokenGenerator = new TokenGeneratorImpl(SESSION_ID_PREFIX, SESSION_ID_SIZE_BYTES);

    public HttpSessionStoreImpl(ServerConfiguration serverConfiguration) {
        microsClock = serverConfiguration.getCairoConfiguration().getMicrosecondClock();
        sessionTimeout = serverConfiguration.getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout();
        rotationPeriod = sessionTimeout / 2;
        rotatedSessionEvictionTime = rotationPeriod / 2;
        evictionCheckInterval = sessionTimeout / 10;
        nextEvictionCheckAt = microsClock.getTicks() + evictionCheckInterval;
    }

    @Override
    public void createSession(@NotNull PrincipalContext principalContext, @NotNull HttpConnectionContext httpContext) {
        // if multiple queries are fired from the client parallel,
        // we can end up creating more sessions for the same client.
        // however, the inactive ones will time out eventually, and will be closed
        final String sessionId = generateSessionId();
        final SessionInfo session = newSession(principalContext, sessionId);
        sessionsById.put(session.getSessionId(), session);
        LOG.info().$("session registered [fd=").$(httpContext.getFd()).$(", principal=").$(session.getPrincipal()).$(']').$();

        final StringSink sessionIdSink = httpContext.getSessionIdSink();
        sessionIdSink.clear();
        sessionIdSink.put(sessionId);
    }

    @Override
    public void destroySession(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext) {
        final SessionInfo session = sessionsById.remove(sessionId);
        if (session != null) {
            session.invalidate();
            LOG.info().$("session destroyed [fd=").$(httpContext.getFd()).$(", principal=").$(session.getPrincipal()).$(']').$();
        }
    }

    @TestOnly
    @Override
    public SessionInfo getSession(@NotNull CharSequence sessionId) {
        return sessionsById.get(sessionId);
    }

    @TestOnly
    @Override
    public void setTokenGenerator(TokenGenerator tokenGenerator) {
        this.tokenGenerator = tokenGenerator;
    }

    @TestOnly
    @Override
    public synchronized int size(@NotNull CharSequence principal) {
        int count = 0;
        for (Map.Entry<CharSequence, SessionInfo> entry : sessionsById.entrySet()) {
            final SessionInfo sessionInfo = entry.getValue();
            if (Chars.equals(principal, sessionInfo.getPrincipal())) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void updateUserGroups(@NotNull CharSequence principal, @NotNull ObjList<CharSequence> groups) {
        final ReadOnlyObjList<CharSequence> currentGroups = groupsByEntity.get(principal);
        if (currentGroups == null) {
            groupsByEntity.put(principal, groups.copy());
            return;
        }

        // ideally these would be compared as sets, but it is ok
        // unlikely that the order of groups changing constantly
        if (!groups.equals(currentGroups)) {
            groupsByEntity.replace(principal, currentGroups, groups.copy());
        }
    }

    @Override
    public SessionInfo verifySessionId(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext) {
        final SessionInfo sessionInfo = sessionsById.get(sessionId);
        if (sessionInfo == null || sessionInfo.isInvalid()) {
            // no valid session
            return null;
        }
        final long currentMicros = microsClock.getTicks();
        if (sessionInfo.getExpiresAt() < currentMicros) {
            // session expired, remove it
            // multiple threads can enter here, destroySession() is threadsafe
            destroySession(sessionId, httpContext);
            return null;
        } else {
            // extend the lifetime of the session
            final long expiresAt = currentMicros + sessionTimeout;
            sessionInfo.setExpiresAt(expiresAt);
        }
        if (sessionInfo.getRotateAt() < currentMicros) {
            // if multiple threads detected that the session id should be rotated,
            // tryLock() makes sure that only one thread will rotate
            if (sessionInfo.tryLock()) {
                try {
                    // check again if we need to rotate
                    // maybe another thread just rotated the session id, and released the lock
                    // after we checked 'rotateAt', but before we acquired the lock
                    if (sessionInfo.getRotateAt() < currentMicros) {
                        final String newSessionId = generateSessionId();
                        final long nextRotationAt = currentMicros + rotationPeriod;
                        sessionInfo.rotate(newSessionId, nextRotationAt);
                        sessionsById.put(newSessionId, sessionInfo);
                        LOG.info().$("session rotated [fd=").$(httpContext.getFd()).$(", principal=").$(sessionInfo.getPrincipal()).$(']').$();

                        final StringSink sessionIdSink = httpContext.getSessionIdSink();
                        sessionIdSink.clear();
                        sessionIdSink.put(newSessionId);
                    }
                } finally {
                    sessionInfo.unlock();
                }
            }
        }
        if (nextEvictionCheckAt < currentMicros) {
            // evict expired sessions periodically
            evictExpiredSessions(currentMicros);
            nextEvictionCheckAt = currentMicros + evictionCheckInterval;
        }
        return sessionInfo;
    }

    /**
     * Removes expired sessions and old rotated session IDs from the map.
     * <p>
     * Two types of entries are evicted:
     * <ol>
     *   <li><b>Expired sessions:</b> SessionInfo.expiresAt has passed</li>
     *   <li><b>Old rotated IDs:</b> Map key differs from SessionInfo.sessionId (indicates an old ID
     *       still pointing to a rotated session), and grace period has expired</li>
     * </ol>
     *
     * <p>For old rotated IDs, the eviction time is calculated as:
     * {@code evictRotatedAt = sessionInfo.rotateAt - rotatedSessionEvictionTime}
     * <p>
     * This works because {@code rotateAt} stores when the <i>next</i> rotation should occur.
     * Since {@code rotationPeriod = 2 * rotatedSessionEvictionTime}, subtracting the grace period
     * from the next rotation time gives us the point when the old ID should be evicted.
     * <p>
     * Example: If session created at T=0, rotated at T=100, next rotation at T=200, grace period is 50:
     * <ul>
     *   <li>T=0: ID1 created, rotateAt = 100</li>
     *   <li>T=100: Rotation occurs (ID1 → ID2), rotateAt = 200</li>
     *   <li>evictRotatedAt = 200 - 50 = 150</li>
     *   <li>T=0 to T=100: ID1 is the current session ID</li>
     *   <li>T=100 to T=150: ID1 is old but still valid (grace period)</li>
     *   <li>T=150+: ID1 evicted from map</li>
     * </ul>
     */
    private synchronized void evictExpiredSessions(long currentMicros) {
        final Iterator<Map.Entry<CharSequence, SessionInfo>> iterator = sessionsById.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<CharSequence, SessionInfo> entry = iterator.next();
            final CharSequence sessionId = entry.getKey();
            final SessionInfo sessionInfo = entry.getValue();
            if (sessionInfo.getExpiresAt() < currentMicros) {
                // expired session
                iterator.remove();
                LOG.info().$("expired session evicted [principal=").$(sessionInfo.getPrincipal()).$(']').$();
            } else if (!Chars.equals(sessionId, sessionInfo.getSessionId())) {
                // Old rotated session ID - the map key doesn't match the current session ID
                // Calculate when this old ID should be evicted by working backwards from next rotation
                final long evictRotatedAt = sessionInfo.getRotateAt() - rotatedSessionEvictionTime;
                if (evictRotatedAt < currentMicros) {
                    // Grace period expired, remove old ID from map
                    iterator.remove();
                    LOG.info().$("rotated session id evicted [principal=").$(sessionInfo.getPrincipal()).$(']').$();
                }
            }
        }
    }

    // although only reads the concurrent map, it is synchronized to protect the
    // token generator which is not threadsafe, and it also protects the collision check
    private synchronized String generateSessionId() {
        for (int i = 0; i < MAX_GENERATION_ATTEMPTS; i++) {
            final CharSequence sessionId = tokenGenerator.newToken();
            if (!sessionsById.containsKey(sessionId)) {
                return sessionId.toString();
            }
        }
        // all attempts led to collisions, unlikely to happen, but fail anyway
        throw CairoException.nonCritical().put("session id collision occurred, try one more time");
    }

    private SessionInfo newSession(@NotNull PrincipalContext principalContext, @NotNull String sessionId) {
        final long currentMicros = microsClock.getTicks();
        return new SessionInfo(
                sessionId,
                Chars.toString(principalContext.getPrincipal()),
                groupsByEntity,
                principalContext.getAuthType(),
                currentMicros + sessionTimeout,
                currentMicros + rotationPeriod
        );
    }
}
