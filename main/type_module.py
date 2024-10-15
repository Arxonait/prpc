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
    def from_str(cls, prefix_with_value: str):
        raise NotImplementedError

    @classmethod
    def check_type(cls, ttype: type):
        return ttype == cls.lib_type

    @classmethod
    def check_structure_serialized_value(cls, serialized_value: str):
        if not isinstance(serialized_value, str):
            raise ValueError(f"input data must be str")

        part_str = serialized_value.split(":", 1)
        if len(part_str) != 2:
            raise ValueError(f"uncorrected structure, value must be 'prefix:value'")


    @classmethod
    def check_prefix(cls, prefix):
        assert prefix is not ...
        if prefix != cls.prefix:
            raise ValueError(f"prefix must be '{cls.prefix}'")


    @classmethod
    def parse_str(cls, prefix_with_value: str):
        cls.check_structure_serialized_value(prefix_with_value)
        prefix, value = prefix_with_value.split(":", 1)
        return prefix, value


class DateTimeType(AbstractLibType):
    prefix = "datetime"
    string_import = "from datetime import datetime"
    lib_type = type(datetime.now())

    @classmethod
    def serialize(cls, value: datetime) -> str:
        return f"{cls.prefix}:{value.isoformat()}"

    @classmethod
    def from_str(cls, prefix_with_value: str):
        prefix, value = cls.parse_str(prefix_with_value)
        cls.check_prefix(prefix)
        return datetime.fromisoformat(value)


class ManagerTypes:
    default_types = [int, float, str, None, bool] # bytes?
    lib_types: list[AbstractLibType] = [
        DateTimeType,
    ]
    #user_types: list[] = []

    @classmethod
    def _search_type(cls, ttype: type):
        if ttype in cls.default_types:
            return True, ttype

        lib_type = cls._search_lib_type_by_type(ttype, False)
        if lib_type:
            return True, lib_type

        return False, None

    @classmethod
    def is_valid_type(cls, ttype: type):
        return cls._search_type(ttype)[0]

    @classmethod
    def is_valid_value(cls, value):
        ttype = type(value)
        return cls.is_valid_type(ttype)

    @classmethod
    def _search_lib_type_by_prefix(cls, prefix: str, raise_exception = False):
        for lib_type in cls.lib_types:
            if lib_type.prefix == prefix:
                return lib_type

        if raise_exception:
            raise Exception(f"not found type by prefix")
        else:
            return None


    @classmethod
    def _search_lib_type_by_type(cls, ttype: type, raise_exception = False):
        for lib_type in cls.lib_types:
            if lib_type.check_type(ttype):
                return lib_type
        if raise_exception:
            raise Exception(f"not found type by type")
        else:
            return None


    @classmethod
    def _recover_value_from_serialize_lib_type(cls, prefix_with_value):
        prefix, value = AbstractLibType.parse_str(prefix_with_value)
        class_type = cls._search_lib_type_by_prefix(prefix)
        return class_type.from_str(prefix_with_value)

    @classmethod
    def serialize_value(cls, value):
        if not cls.is_valid_value(value):
            raise Exception("") # todo

        if type(value) in cls.default_types:
            return value

        lib_type = cls._search_lib_type_by_type(type(value))
        if lib_type:
            return lib_type.serialize(value)

        raise Exception # todo

    @classmethod
    def recover_value(cls, value):
        if isinstance(value, str):
            try:
                AbstractLibType.check_structure_serialized_value(value)
                return cls._recover_value_from_serialize_lib_type(value)
            except ValueError as e:
                print(e)
                return value

        if type(value) in cls.default_types:
            return value

        raise Exception # todo


