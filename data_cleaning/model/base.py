"""base class for each component and storage to be defined here"""

from abc import ABC, abstractmethod
from typing import Optional

from helper.logger import Logger

class DataProcessingComponent(ABC):
    def __init__(self, debug: bool = False):
        self.debug = debug

    @abstractmethod
    def process(self, content: str, logger: Logger, filename: str) -> Optional[str]:
        pass

class DataStorageComponent(ABC):
    @abstractmethod
    def save(self, key: str, content: str, subdir_name: str, logger: Logger) -> None:
        pass