import json
import uuid

import pytest

from prpc.prpcmessage import PRPCMessage
from prpc.support_module.exceptions import PRPCMessageDeserializeError, JSONDeserializeError


class TestPrpcMessage:
    @pytest.mark.parametrize(
        "input_data, expected_exception", [
            (json.dumps({"func_name": "summ"}), PRPCMessageDeserializeError),
            ("hello_world", JSONDeserializeError)
        ])
    def test_deserialize_with_exception(self, input_data, expected_exception):
        with pytest.raises(expected_exception) as e_info:
            PRPCMessage.deserialize(input_data)

    def test_deserialize(self):
        PRPCMessage.deserialize(PRPCMessage("summ", None, None).serialize())

    @pytest.mark.parametrize(
        "input_data, expected_exception", [
            (json.dumps({"func_name": "summ"}), PRPCMessageDeserializeError),
            (json.dumps({"name_func": "summ", "message_id": str(uuid.uuid4())}), PRPCMessageDeserializeError),
            (json.dumps({"func_name": "summ", "message_id": "foo-123"}), PRPCMessageDeserializeError),
            (PRPCMessage("summ", None, None).serialize(), PRPCMessageDeserializeError),
            ("hello_world", JSONDeserializeError)
        ])
    def test_deserialize_raw_with_exception(self, input_data, expected_exception):
        with pytest.raises(expected_exception) as e_info:
            PRPCMessage.deserialize_raw(input_data)

    def test_deserialize_raw(self):
        PRPCMessage.deserialize_raw(json.dumps({"func_name": "summ", "message_id": str(uuid.uuid4())}))
