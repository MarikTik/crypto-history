"""
This module defines the NullLogger class, a utility for handling optional
logging or similar behaviors where method calls should be silently ignored
when an object is in an uninitialized or "null" state.

The NullLogger class is particularly useful in scenarios where:

1.  Logging is optional and may not be configured in all environments.
2.  One desires to avoid clustering code with `if logger is not None` checks.

Example Usage:

    from null_logger import NullLogger

    class MyClass:
        def __init__(self):
            self.logger = NullLogger()  # Initialize with NullLogger

        def do_something(self):
            # ... some logic ...
            self.logger.info("This log will be ignored if logger is not configured.")
            self.logger.debug("Debug messages are also ignored.")

        def configure_logger(self, real_logger):
            # Replace NullLogger with a real logger when available.
            self.logger = real_logger

    obj = MyClass()
    obj.do_something()  # No errors, log calls are ignored.

    # Later, configure a real logger:
    import logging
    logging.basicConfig(level=logging.INFO)
    real_logger = logging.getLogger(__name__)
    obj.configure_logger(real_logger)
    obj.do_something()  # Now, log calls will be processed.
"""


class NullLogger:
    """
    A logger-like object that silently absorbs any method calls.

    This class is designed to act as a placeholder when a real logger is not
    available or has not been configured. It prevents errors by silently
    ignoring any method calls made to it.
    """

    def __getattr__(self, name):
        """
        Silently absorb any method calls.

        This method is called when an attribute (method) is accessed that does
        not exist on the NullLogger instance. It returns the noop method, which
        effectively ignores the call.
        """
        return self.noop

    def noop(self, *args, **kwargs):
        """
        Does nothing.

        This method is returned by __getattr__ and is called when a method is
        called on the NullLogger. It simply does nothing, effectively
        ignoring the method call.
        """
        pass
