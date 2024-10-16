

class CheckerAccessType:
    base_module = 'builtins'
    lib_module = ('datetime', 'uuid')
    specific_type: tuple[type] = ()

    @classmethod
    def _get_module_form_value(cls, value):
        return type(value).__module__

    @classmethod
    def check_type(cls, value):
        module = cls._get_module_form_value(value)
        if module == cls.base_module or module in cls.lib_module or type(value) in cls.specific_type:
            return True
        return False

    @classmethod
    def check_obj(cls, value):
        if isinstance(value, tuple | list | set | dict):
            if isinstance(value, dict):
                value = value.items()
            result_mass = []
            result = True
            for item in value:
                result_iter, mass = cls.check_obj(item)
                result_mass.extend(mass)
                result = result and result_iter
            return result, result_mass

        if cls.check_type(value):
            return True, []
        return False, [value]
