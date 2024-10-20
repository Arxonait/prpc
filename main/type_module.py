import re
from typing import get_args, get_origin

# todo refactoring

BASE_MODULE = ('builtins', 'typing')
LIB_MODULE = ('datetime', 'uuid')


class CheckerValueSerialize:
    specific_type: tuple[type] = ()

    @classmethod
    def _get_module_from_value(cls, value):
        return type(value).__module__

    @classmethod
    def _check_value_for_serialize(cls, value):
        module = cls._get_module_from_value(value)
        if module in (BASE_MODULE + LIB_MODULE) or type(value) in cls.specific_type:
            return True
        return False

    @classmethod
    def is_value_good_for_serialize(cls, value) -> tuple[bool, list]:

        if isinstance(value, tuple | list | set | dict):
            if isinstance(value, dict):
                value = value.items()
            result_mass = []
            result = True
            for item in value:
                result_iter, mass = cls.is_value_good_for_serialize(item)
                result_mass.extend(mass)
                result = result and result_iter
            return result, result_mass

        if cls._check_value_for_serialize(value):
            return True, []

        return False, [value]


class HandlerAnnotation:

    @classmethod
    def _annotation_to_str(cls, annotation: type):
        annotation = str(annotation)
        result = re.search(r"<class '(.+)'>", annotation)
        if result:
            annotation = result.group(1)
        return annotation

    @classmethod
    def _remove_annotation_module(cls, annotation_str: str):
        for removed_annotation_module in (LIB_MODULE + BASE_MODULE):
            annotation_str = annotation_str.replace(removed_annotation_module + ".", "")
        return annotation_str

    @classmethod
    def serialize_annotation(cls, annotation: type) -> str:
        annotation = cls._annotation_to_str(annotation)
        annotation = cls._remove_annotation_module(annotation)
        return annotation

    @classmethod
    def is_valid_annotation(cls, annotation):
        """Рекурсивно проверяем, является ли аннотация допустимой."""
        invalid_types: list = []

        # Если аннотация простой тип
        if isinstance(annotation, type) and annotation.__module__ in (BASE_MODULE + LIB_MODULE):
            return invalid_types

        # Если аннотация это не тип а значение
        if not isinstance(annotation, type) and get_origin(annotation) is None:
            if isinstance(annotation, (list, set, tuple, dict)):
                for item in annotation:
                    invalid_types.extend(cls.is_valid_annotation(item))
            else:
                invalid_types.extend(cls.is_valid_annotation(type(annotation)))

            return invalid_types

        # Если аннотация сложный тип
        if get_origin(annotation):
            for arg in get_args(annotation):
                invalid_types.extend(cls.is_valid_annotation(arg))
            return invalid_types

        # Если тип некорректен, добавляем его в список
        invalid_types.append(annotation)
        return invalid_types
