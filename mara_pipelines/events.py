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
