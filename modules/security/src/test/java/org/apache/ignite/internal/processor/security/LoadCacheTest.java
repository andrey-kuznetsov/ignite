package org.apache.ignite.internal.processor.security;

import java.util.UUID;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for cache data load.
 */
public class LoadCacheTest extends AbstractContextResolverSecurityProcessorTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<String, Integer>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false),
            new CacheConfiguration<Integer, Integer>()
                .setName(SEC_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false)
                .setCacheStoreFactory(new TestStoreFactory())
        };
    }

    /** */
    public void testLoadCache() {
        successLoad(succsessClnt, succsessSrv);
        successLoad(succsessClnt, failSrv);
        successLoad(succsessSrv, succsessSrv);
        successLoad(succsessSrv, failSrv);

        failLoad(failClnt, succsessSrv);
        failLoad(failSrv, succsessSrv);
        failLoad(failSrv, failSrv);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successLoad(IgniteEx initiator, IgniteEx remote) {
        assert !remote.localNode().isClient();

        Integer val = values.getAndIncrement();

        initiator.<Integer, Integer>cache(SEC_CACHE_NAME).loadCache(
            new TestClosure(remote.localNode().id(), "key", val)
        );

        assertThat(succsessSrv.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void failLoad(IgniteEx initiator, IgniteEx remote) {
        assert !remote.localNode().isClient();

        assertCauseMessage(
            GridTestUtils.assertThrowsWithCause(
                () -> initiator.<Integer, Integer>cache(SEC_CACHE_NAME)
                    .loadCache(
                        new TestClosure(remote.localNode().id(), "fail_key", -1)
                    )
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements IgniteBiPredicate<Integer, Integer> {
        /** Remote node id. */
        private final UUID remoteId;

        /** Key. */
        private final String key;

        /** Value. */
        private final Integer val;

        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /**
         * @param remoteId Remote id.
         * @param key Key.
         * @param val Value.
         */
        public TestClosure(UUID remoteId, String key, Integer val) {
            this.remoteId = remoteId;
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer k, Integer v) {
            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_NAME).put(key, val);

            return false;
        }
    }

    /**
     * Test store factory.
     */
    private static class TestStoreFactory implements Factory<TestCacheStore> {
        /** {@inheritDoc} */
        @Override public TestCacheStore create() {
            return new TestCacheStore();
        }
    }

    /**
     * Test cache store.
     */
    private static class TestCacheStore extends CacheStoreAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, Object... args) {
            clo.apply(1, 1);
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}
