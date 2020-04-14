import json
import abc
import datetime
import sys


class Event():
    def __init__(self) -> None:
        """
        Base class for events that are emitted from mara.
        """
        pass

    def to_json(self):
        return json.dumps({field: value.isoformat() if isinstance(value, datetime.datetime) else value
                           for field, value in self.__dict__.items()})


class GenericExceptionEvent(Event):
    def __init__(self, exception: BaseException, msg:str) -> None:
        """
        A generic Exception occurred during execution of the pipeline which could not be handled

        Args:
            exception: The BaseException
            msg: an additional message
        """
        import traceback
        super().__init__()
        self.exception_type: str = str(type(exception).__name__)
        self.exception_msg: str = str(exception)
        self.exception_traceback: str = traceback.format_exc()
        self.msg = msg
        # make it easier in the web UI by precomputing it
        if self.msg:
            self._repr = f"{self.msg}: {self.exception_type}({self.exception_msg})"
        else:
            self._repr =  f"{self.exception_type}({self.exception_msg})"

    def __repr__(self):
        return self._repr


class EventHandler(abc.ABC):
    @abc.abstractmethod
    def handle_event(self, event: Event):
        pass


def notify_configured_event_handlers(event: Event):
    from . import config
    try:
        all_handlers = config.event_handlers()
    except BaseException as e:
        print(f"Exception while getting configured event handlers: {repr(e)}", file=sys.stderr)
        return

    for handler in all_handlers:
        try:
            handler.handle_event(event)
        except BaseException as e:
            print(f"Handler {repr(handler)} could report about {repr(event)}: {repr(e)}", file=sys.stderr)
