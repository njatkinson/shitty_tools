import time
from functools import wraps
from contextlib import contextmanager
from Queue import Queue
import concurrent


def construct_rate_limit_context(hz, slack = 1, rate_limit_queue_class = Queue):
    '''
    This function returns a context manager that allows you to rate limit your shitty software.

    It's thread safe.

    The rate limit will be applied in aggregate across all threads of execution.
    So, if you set the rate limit to 1 hz, you'll only be able to enter the rate limited context
    once per second whether you have 1 thread or 50 threads.

    You can use this in multiple processes as well.
    Just specify rate_limit_queue_class = multiprocessing.Queue
    Bam, you're multiprocessing.

    You can specify some other kind of queue class if you want. I don't give a shit. Maybe you
    want to use RabbitMQ or Redis to build a distributed rate limiter, or a queue that counts
    how many times you put shit in it, or something from shitty_tools.queue.

    The only rules for custom queue class is that you need to be able to instantiate it without
    arguments. The underlying code is simply going to call your_shitty_queue(). So, if instantiation
    time arguments are needed, you'll need to figure it out. Try something like this--
    construct_rate_limit(1, rate_limit_queue_class = lambda: some_dumbshit_queue_class(some_bullshit_argument))

    You can use whatever bullshit class you want for the queue class. It just needs to implement
    get and put methods.

    You're probably wondering now what the fuck the "slack" parameter is for. It allows you to
    let your system handle a small burst of request before the rate limiter kicks in. Say you set
    slack to 10 and hz to 1. At start time, your application will now be able to enter the rate
    limited context 10 times immediately. The 11th time your application tries to enter the context,
    it will block until 1 second has passed since the first time the context was exited.

    Slack is replenished at the rate specified by hz. So, if hz = 1 slack will increase by 1 every
    second until the maximum slack size is reached.

    Slack is useful if you want your application to be able to burst but then run sustained at a
    lower rate. It's also useful if you're running in a highly threaded environment at high hz.
    So, if you're running in a highly threaded (or multiprocess) environment and the rate limiter
    seems to be restricting the rate more than it should, try bumping up the slack.


    >>> r = construct_rate_limit_context(1)
    >>> for i in range(3):
    ...     with r(): print i
    ...
    0
    1
    2

    :param hz: Number of times per second context may be entered
    :param slack: Slack in the system (see docs)
    :param rate_limit_queue_class: A function/class that returns something queue like
    :return:
    '''
    action_queue = rate_limit_queue_class()
    wait_queue = rate_limit_queue_class()
    sleep_time = 1.0 / hz

    def wait():
        while True:
            wait_queue.get()
            time.sleep(sleep_time)
            action_queue.put(None)

    wait_thread = concurrent.construct_daemon_thread(wait)
    wait_thread.start()
    for i in xrange(slack or 1):
        action_queue.put(None)

    @contextmanager
    def rate_limit_context():
        action_queue.get()
        yield
        wait_queue.put(None)
    return rate_limit_context


def rate_limit_decorator(rate_limit_context):
    '''
    This is for decorating your shitty functions/methods to limit their rate. You need to
    pass in a rate limit context for the function.

    Example:

    >>> r = construct_rate_limit_context(1)
    >>> @rate_limit_decorator(r)
    ... def do_nothing(x): return x
    ...
    >>> map(do_nothing, range(3))
    [0, 1, 2]


    :param rate_limit_context:
    :return:
    '''
    def outer(f):
        @wraps(f)
        def inner(*args, **kwargs):
            with rate_limit_context():
                return f(*args, **kwargs)
        return inner
    return outer