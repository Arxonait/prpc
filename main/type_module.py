from abc import ABC, abstractmethod
from datetime import datetime


class AbstractLibType(ABC):
    prefix: str = ...
    string_import: str = ...
    lib_type: type = ...

    @classmethod
    @abstractmethod
    def serialize(cls, value) -> str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_str(cls, string: str):
        raise NotImplementedError

    @classmethod
    def check_type(cls, ttype: type):
        return ttype == cls.lib_type

    @classmethod
    def parse_str(cls, string: str):
        part_str = string.split(":", 1)
        if len(part_str) != 2:
            raise ValueError(f"not found prefix {cls.prefix}")

        if part_str[0] != cls.prefix:
            raise ValueError(f"uncorrected prefix {part_str[0]}, waited {cls.prefix}")

        return part_str[0], part_str[1]


class DateTimeType(AbstractLibType):
    prefix = "datetime"
    string_import = "from datetime import datetime"
    lib_type = type(datetime)

    @classmethod
    def serialize(cls, value: datetime) -> str:
        return f"{cls.prefix}:{value.isoformat()}"

    @classmethod
    def from_str(cls, string: str):
        prefix, value = cls.parse_str(string)
        return datetime.fromisoformat(value)


class ManagerTypes:
    default_types = [int, float, str, None, bool] # bytes?
    lib_types: list[AbstractLibType] = [
        DateTimeType,
    ]
    #user_types: list[] = []

    @classmethod
    def search_type(cls, ttype: type):
        if ttype in cls.default_types:
            return True, ttype

        for lib_type in cls.lib_types:
            if lib_type.check_type(ttype):
                return True, lib_type

        return False, None

    @classmethod
    def is_valid_type(cls, ttype: type):
        return cls.search_type(ttype)[0]

    @classmethod
    def recover_value(cls, value):
        prefix, value = AbstractLibType.parse_str(value)
        current_lib_type = None
        for lib_type in cls.lib_types:
            if lib_type.prefix == prefix:
                current_lib_type = lib_type
                break

        if current_lib_type is None:
            ...

    @classmethod
    def is_valid_value(cls, value):
        ttype = type(value)
        return cls.is_valid_type(ttype)