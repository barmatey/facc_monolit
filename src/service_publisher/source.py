from abc import ABC, abstractmethod
from report import service_crud

from wire import entities as entities_wire
from report import entities as entities_report


class Subscriber(ABC):

    @abstractmethod
    def total_recalculate(self, source: entities_wire.Source):
        pass


class ReportSubscriber(Subscriber):
    async def total_recalculate(self, source: entities_wire.Source):
        group_service = service_crud.GroupService()
        linked_groups: list[entities_report.Group] = await group_service.retrieve_bulk({"source_id": source.id})

        for group in linked_groups:
            await group_service.total_recalculate(group)


class SourcePublisher:

    def __init__(self, source: entities_wire.Source):
        self.__subscribers = set()
        self.source = source.copy()

    def subscribe(self, subscriber: Subscriber):
        self.__subscribers.add(subscriber)

    def unsubscribe(self, subscriber: Subscriber):
        self.__subscribers.remove(subscriber)

    def notify_total_recalculate(self):
        for subscriber in self.__subscribers:
            subscriber.update_data()
