package com.mythfish.shiro.cache.memcached;

import java.io.IOException;
import java.util.Map;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.config.ConfigurationException;
import org.apache.shiro.config.Ini;
import org.apache.shiro.io.ResourceUtils;
import org.apache.shiro.util.Destroyable;
import org.apache.shiro.util.Initializable;
import org.apache.shiro.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class MemcachedManager implements CacheManager, Initializable, Destroyable {

    /**
     * This class's private log instance.
     */
    private static final Logger log = LoggerFactory.getLogger(MemcachedManager.class);

    public static final String CONFIG_SESSION_MAIN = "main";
    public static final String CONFIG_PROP_SERVERS = "servers";
    
    /**
     * The Memcached cache manager used by this implementation to create caches.
     */
    protected Map<String, MemcachedClient> clients = Maps.newConcurrentMap();
    protected MemcachedClientBuilder builder = null;

    /**
     * Indicates if the CacheManager instance was implicitly/automatically created by this instance, indicating that
     * it should be automatically cleaned up as well on shutdown.
     */
    private boolean cacheManagerImplicitlyCreated = false;

    /**
     * Classpath file location of the memcached CacheManager config file.
     */
    private String cacheManagerConfigFile = "classpath:com/rzico/grape/sc/shiro/cache/memcached/memcached.ini";

    /**
     *	memcached ini configure. 
     */
    private Ini ini = null;
    
    /**
     * Default no argument constructor
     */
    public MemcachedManager() {
    }

    /**
     * Returns the resource location of the config file used to initialize a new
     * EhCache CacheManager instance.  The string can be any resource path supported by the
     * {@link org.apache.shiro.io.ResourceUtils#getInputStreamForPath(String)} call.
     * <p/>
     * This property is ignored if the CacheManager instance is injected directly - that is, it is only used to
     * lazily create a CacheManager if one is not already provided.
     *
     * @return the resource location of the config file used to initialize the wrapped
     *         EhCache CacheManager instance.
     */
    public String getCacheManagerConfigFile() {
        return this.cacheManagerConfigFile;
    }

    /**
     * Sets the resource location of the config file used to initialize the wrapped
     * EhCache CacheManager instance.  The string can be any resource path supported by the
     * {@link org.apache.shiro.io.ResourceUtils#getInputStreamForPath(String)} call.
     * <p/>
     * This property is ignored if the CacheManager instance is injected directly - that is, it is only used to
     * lazily create a CacheManager if one is not already provided.
     *
     * @param classpathLocation resource location of the config file used to create the wrapped
     *                          EhCache CacheManager instance.
     */
    public void setCacheManagerConfigFile(String classpathLocation) {
        this.cacheManagerConfigFile = classpathLocation;
    }

    /**
     * Acquires the Ini for the ehcache configuration file using
     * {@link ResourceUtils#getInputStreamForPath(String) ResourceUtils.getInputStreamForPath} with the
     * path returned from {@link #getCacheManagerConfigFile() getCacheManagerConfigFile()}.
     *
     * @return the InputStream for the ehcache configuration file.
     */
    protected void cacheManagerConfigFileToIni() {
        String configFile = getCacheManagerConfigFile();
        try {
        	ini = Ini.fromResourcePath(configFile);
        } catch (ConfigurationException e) {
        	log.error("Read configuration to Ini error.");
        }        
    }

    /**
     * Loads an existing Memcached from the cache manager, or starts a new cache if one is not found.
     *
     * @param name the name of the cache to load/create.
     */
    public final <K, V> Cache<K, V> getCache(String name) throws CacheException {

        if (log.isTraceEnabled()) {
            log.trace("Acquiring Memcached instance named [" + name + "]");
        }

        try {
             ensureCacheManager();
             MemcachedClient cache = this.clients.get(name);
            if (cache == null) {
                if (log.isInfoEnabled()) {
                    log.info("Cache with name '{}' does not yet exist.  Creating now.", name);
                }
                cache = this.builder.build();
                cache.setName(name);
                this.clients.put(name, cache);

                if (log.isInfoEnabled()) {
                    log.info("Added Memcached named [" + name + "]");
                }
            } else {
                if (log.isInfoEnabled()) {
                    log.info("Using existing Memcached named [" + cache.getName() + "]");
                }
            }
            return new Memcached<K, V>(cache);
        } catch (IOException e) {
            throw new CacheException(e);
        }
    }

    /**
     * Initializes this instance.
     * <p/>
     * If a {@link #setCacheManager CacheManager} has been
     * explicitly set (e.g. via Dependency Injection or programatically) prior to calling this
     * method, this method does nothing.
     * <p/>
     * However, if no {@code CacheManager} has been set, the default Ehcache singleton will be initialized, where
     * Ehcache will look for an {@code ehcache.xml} file at the root of the classpath.  If one is not found,
     * Ehcache will use its own failsafe configuration file.
     * <p/>
     * Because Shiro cannot use the failsafe defaults (fail-safe expunges cached objects after 2 minutes,
     * something not desirable for Shiro sessions), this class manages an internal default configuration for
     * this case.
     *
     * @throws org.apache.shiro.cache.CacheException
     *          if there are any CacheExceptions thrown by EhCache.
     * @see net.sf.ehcache.CacheManager#create
     */
    public final void init() throws CacheException {
        ensureCacheManager();
    }

    private void ensureCacheManager() throws CacheException {
        try {
            if (this.clients == null) {
                if (log.isDebugEnabled()) {
                    log.debug("cacheManager property not set.  Constructing CacheManager instance... ");
                }
                if (this.ini == null) {
                	cacheManagerConfigFileToIni();
                }
                
                this.clients = Maps.newConcurrentMap();
                
                String servers = ini.getSectionProperty(CONFIG_SESSION_MAIN, CONFIG_PROP_SERVERS);
                if (!StringUtils.hasText(servers)) {
                	String error = "Not servers at configure file."; 
                	log.error(error);
                	throw new CacheException(error);
                }
                this.builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(StringUtils.clean(servers)));
                // Todo: get configure parameters from ini;
//                builder.setConnectTimeout(connectTimeout);
                
                if (log.isTraceEnabled()) {
                    log.trace("instantiated Memcached CacheManager instance.");
                }
                cacheManagerImplicitlyCreated = true;
                if (log.isDebugEnabled()) {
                    log.debug("implicit cacheManager created successfully.");
                }
            }
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    /**
     * Shuts-down the wrapped Ehcache CacheManager <b>only if implicitly created</b>.
     * <p/>
     * If another component injected
     * a non-null CacheManager into this instace before calling {@link #init() init}, this instance expects that same
     * component to also destroy the CacheManager instance, and it will not attempt to do so.
     */
    public void destroy() {
        if (cacheManagerImplicitlyCreated) {
            try {
            	this.clients = null;
            	this.ini = null;
            	this.builder = null;
            } catch (Exception e) {
                if (log.isWarnEnabled()) {
                    log.warn("Unable to cleanly shutdown implicitly created CacheManager instance.  " +
                            "Ignoring (shutting down)...");
                }
            }
            cacheManagerImplicitlyCreated = false;
        }
    }
}
