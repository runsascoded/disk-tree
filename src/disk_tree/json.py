from dataclasses import is_dataclass, asdict
from datetime import datetime
from json import JSONEncoder


class Encoder(JSONEncoder):
    def default(self, o):
        if is_dataclass(o):
            return asdict(o)
        elif isinstance(o, datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return super().default(o)
