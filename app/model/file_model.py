from dataclasses import dataclass
from typing import Optional


@dataclass
class FileModel:
    name: str
    path: str
    content: Optional[str]