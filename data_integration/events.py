import json
import abc
import datetime


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
    all_handlers = config.event_handlers()
    for handler in all_handlers:
        handler.handle_event(event)
