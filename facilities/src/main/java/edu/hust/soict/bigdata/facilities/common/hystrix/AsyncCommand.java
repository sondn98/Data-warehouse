package edu.hust.soict.bigdata.facilities.common.hystrix;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixObservableCommand;
import rx.Observable;

import java.util.concurrent.Callable;

public class AsyncCommand<T> extends HystrixObservableCommand<T> {

    private Callable<T> callable;
    private T fallback;

    protected AsyncCommand(String group, String name, Callable<T> callable, T fallback) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(group)).
                andCommandKey(HystrixCommandKey.Factory.asKey(name)));
        this.callable = callable;
        this.fallback = fallback;
    }

    public AsyncCommand(String group, String name, Callable<T> callable) {
        this(group, name, callable, null);
    }

    @Override
    protected Observable<T> construct() {
        return Observable.create(subscriber -> {
            try {
                subscriber.onNext(callable.call());
                subscriber.onCompleted();
            } catch (Exception e) {
                subscriber.onError(e);
            }
        });
    }

    public T getFallback() {
        return fallback;
    }
}
