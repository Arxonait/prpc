class NotFoundFunc(Exception):
    def __init__(self, func_name: str):
        self.func_name = func_name

    def __str__(self):
        return f"func name '{self.func_name}' not found in server"


class PRPCMessageDeserializeError(Exception):
    def __init__(self, serialize_message: str):
        self.serialize_message = serialize_message

    def __str__(self):
        return f"the message {self.serialize_message} has the wrong format for deserialize prpc message"


class MessageFromStreamDataValidationError(Exception):
    def __str__(self):
        return f"the message from stream does not have an attribute `message`"


class JSONDeserializeError(Exception):
    def __str__(self):
        return f"json error"
