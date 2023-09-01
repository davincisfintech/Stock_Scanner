from abc import ABCMeta, abstractmethod


class DataClient(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, *args, **kwargs):
        pass
