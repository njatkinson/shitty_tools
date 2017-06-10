import threading
from Queue import Queue


def construct_daemon_thread(f):
    '''
    Convenience function that takes a function 'f' and returns:

     f_thread = threading.Thread(target = f)
     f_thread.daemon = True
     return f_thread

    This is only needed because threading.Thread won't let you set daemon status
    in the constructor.

    :param f: some function
    :return: a thread that can execute the function
    '''
    f_thread = threading.Thread(target = f)
    f_thread.daemon = True
    return f_thread


def queue_wrap_function(input_queue, output_queue, f):
    '''
    Given input and output queues and a function, constructs a thread that
    executes f on values from the input queue and puts the results into the
    output queue.

    f is a function that accepts one argument.

    :param input_queue: Something queue-like that supports .get()
    :param output_queue: Something queue-like that supports .put()
    :param f: A function
    :return: A running thread that applies f to input and puts the
    results in output
    '''
    def thread_fun():
        while True:
            output_queue.put(f(input_queue.get()))

    f_thread = construct_daemon_thread(thread_fun)
    f_thread.start()
    return f_thread


def construct_pipeline(function_list, queue_class = Queue):
    '''
    Takes a list of functions, spawns one thread for each function in the list,
    connects them with queues, and returns a function that gives a value to the
    pipeline and returns the output of the pipeline.

    Specify some other shitty kind of queue if you want. It needs to be
    instatiatable with zero arguments (wrap in lambda if needed), and it needs to
    support .get() and .put().

    >>> do_nothing = lambda x: x
    >>> pipeline = construct_pipeline([do_nothing, do_nothing, do_nothing])
    >>> pipeline('This is shitty')
    'This is shitty'


    :param function_list:
    :param queue_class:
    :return: function to pass value into pipeline and return result
    '''
    q_list = [queue_class() for i in xrange(len(function_list) + 1)]
    for input_q, output_q, f in zip(q_list, q_list[1::], function_list):
        queue_wrap_function(input_q, output_q, f)

    def io_fun(arg):
        q_list[0].put(arg)
        result = q_list[-1].get()
        return result

    return io_fun


def construct_loop(function_list, queue_class = Queue):
    '''
    Takes a list of functions and constructs a pipeline that feeds its output
    back into itself. Returns a function that allows you to pass a value into
    the pipeline to kick it off.

    If you want to you can invoke the returned function multiple times to allow
    multiple values in the pipeline at the same time.

    >>> def drop_last_char(some_str): return some_str[:-1]
    ...
    >>> def print_some_str_maybe(some_str):
    ...     if some_str: print(some_str)
    ...     return some_str
    ...
    >>> loop = construct_loop([drop_last_char, print_some_str_maybe])
    >>> loop('Hello')
    Hell
    Hel
    He
    H

    :param function_list:
    :param queue_class:
    :return: function to push value into the loop
    '''
    pipeline = construct_pipeline(function_list, queue_class)
    loop_q = queue_class()

    def run_loop():
        while True:
            loop_q.put(pipeline(loop_q.get()))

    loop_thread = construct_daemon_thread(run_loop)
    loop_thread.start()

    return loop_q.put