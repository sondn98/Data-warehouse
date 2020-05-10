package edu.hust.soict.bigdata.facilities.common.hystrix;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import edu.hust.soict.bigdata.facilities.common.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * @author sondn
 * @since 1/4/2020
 **/

public class SyncCommand<T> extends HystrixCommand<T> {
    private static final Logger logger = LoggerFactory.getLogger(SyncCommand.class);
    private Callable<T> callable;
    private T fallback;
    private boolean throwable;

    /**
     * I am lazy mam, so I don't want to pass properties to here
     **/
    protected SyncCommand(String group, String name, Callable<T> callable, T fallback, boolean throwable) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(group))
                .andCommandKey(HystrixCommandKey.Factory.asKey(name))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionTimeoutInMilliseconds(5000)
                        .withCircuitBreakerEnabled(true)
                        .withCircuitBreakerRequestVolumeThreshold(10)
                        .withCircuitBreakerSleepWindowInMilliseconds(10000)
                ));
        this.callable = callable;
        this.fallback = fallback;
        this.throwable = throwable;
    }

    public SyncCommand(String group, String name, Callable<T> callable) {
        this(group, name, callable, null, true);
    }

    public SyncCommand(String group, String name, Callable<T> callable, boolean throwable) {
        this(group, name, callable, null, throwable);
    }

    @Override
    protected T run() throws Exception {
        try {
            return callable.call();
        } catch (Throwable e) {
            if (throwable) {
                if (e instanceof ClientException) {
                    throw new HystrixBadRequestException(e.getMessage());
                } else throw e;
            }else {
                logger.error(e.getMessage(), e);
            }
        }
        return null;
    }
}
