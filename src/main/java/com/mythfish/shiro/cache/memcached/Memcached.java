package com.mythfish.shiro.cache.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shiro {@link org.apache.shiro.cache.Cache} implementation that wraps an {@link net.sf.ehcache.Ehcache} instance.
 *
 * @since 0.2
 */
public class Memcached<K, V> implements Cache<K, V> {

    /**
     * Private internal log instance.
     */
    private static final Logger log = LoggerFactory.getLogger(Memcached.class);

    /**
     * The wrapped MemcachedClient instance.
     */
    private MemcachedClient cache;

    /**
     * Constructs a new Memcached instance with the given cache.
     *
     * @param cache - delegate Memcached instance this Shiro cache instance will wrap.
     */
    public Memcached() {
    	MemcachedClientBuilder builder = new XMemcachedClientBuilder();
    	try {
			this.cache = builder.build();
		} catch (IOException e) {
			log.error("Builder memcached client error. cause: " + e.getStackTrace().toString());
			throw new CacheException(e);
		}
    }
    
    /**
     * Constructs a new Memcached instance with the given cache.
     *
     * @param cache - delegate Memcached instance this Shiro cache instance will wrap.
     */
    public Memcached(MemcachedClient cache) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache argument cannot be null.");
        }
        this.cache = cache;
    }
    
    public void setCacheName(String name) {
    	if (StringUtils.hasText(name)) {
    		this.cache.setName(StringUtils.clean(name));
    	}
    }
    
    public void setCacheServers(String servers) {
    	for (InetSocketAddress host : AddrUtil.getAddresses(StringUtils.clean(servers))) {
			try {
				this.cache.addServer(host);
			} catch (IOException e) {
				log.error("Add server [" + host + "] to memcached client error when set servers [" + StringUtils.clean(servers) + "]");
				throw new CacheException("Cache argument cannot be null.");
			}
    	}
    }

    /**
     * Gets a value of an element which matches the given key.
     *
     * @param key the key of the element to return.
     * @return The value placed into the cache with an earlier put, or null if not found or expired
     */
    public V get(K key) throws CacheException {
        try {
            if (log.isTraceEnabled()) {
                log.trace("Getting object from cache [" + cache.getName() + "] for key [" + key + "]");
            }
            if (key == null) {
                return null;
            } else {
                return cache.get(key.toString());
            }
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    /**
     * Puts an object into the cache.
     *
     * @param key   the key.
     * @param value the value.
     */
    public V put(K key, V value) throws CacheException {
        if (log.isTraceEnabled()) {
            log.trace("Putting object in cache [" + cache.getName() + "] for key [" + key + "]");
        }
        try {
            V previous = get(key);
            cache.set(key.toString(), 0, value);
            return previous;
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    /**
     * Removes the element which matches the key.
     *
     * <p>If no element matches, nothing is removed and no Exception is thrown.</p>
     *
     * @param key the key of the element to remove
     */
    public V remove(K key) throws CacheException {
        if (log.isTraceEnabled()) {
            log.trace("Removing object from cache [" + cache.getName() + "] for key [" + key + "]");
        }
        try {
            V previous = get(key);
            cache.delete(key.toString());
            return previous;
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    /**
     * Removes all elements in the cache, but leaves the cache in a useable state.
     */
    public void clear() throws CacheException {
        if (log.isTraceEnabled()) {
            log.trace("Clearing all objects from cache [" + cache.getName() + "]");
        }
        try {
            cache.flushAllWithNoReply();
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    public int size() {
        try {
        	//TODO: unsupported
            return 0;
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    public Set<K> keys() {
        try {
        	//TODO: unsupported
        	return Collections.emptySet();
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    public Collection<V> values() {
        try {
            //TODO: unsupported
            return Collections.emptyList();
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    /**
     * Returns &quot;Memcache [&quot; + cache.getName() + &quot;]&quot;
     *
     * @return &quot;Memcache [&quot; + cache.getName() + &quot;]&quot;
     */
    public String toString() {
        return "Memcache [" + cache.getName() + "]";
    }
}
