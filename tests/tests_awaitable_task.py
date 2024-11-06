import datetime
from copy import copy
from unittest.mock import Mock, patch

import pytest

from prpc.app_client import AwaitableTask
from prpc.prpcmessage import PRPCMessage


@pytest.fixture()
def get_mocks():
    mock_client_for_get_result = Mock()
    mock_client_for_get_result.search_message_in_feedback.return_value = None
    mock_client_for_get_result.close.return_value = None

    mock_client_for_send_message = Mock()
    mock_client_for_send_message.send_message.return_value = None
    mock_client_for_send_message.client_broker_class.return_value = mock_client_for_get_result
    return mock_client_for_get_result, mock_client_for_send_message


@pytest.fixture()
def get_prpc_message():
    message = PRPCMessage("foo", (), {})
    message_done = copy(message)
    message_done.message_to_done(result="foo")

    return message, message_done


class TestAwaitableTask:

    def test_send_message_by_create_awaitable_task(self, get_mocks):
        mock_client_for_get_result, mock_client_for_send_message = get_mocks

        with patch("prpc.app_client.ClientForSendMessage.get_instance", return_value=mock_client_for_send_message):
            task = AwaitableTask("foo", (), {})
            mock_client_for_send_message.send_message.assert_called_once()

    def test_update_task_done(self, get_mocks, get_prpc_message):
        mock_client_for_get_result, mock_client_for_send_message = get_mocks
        message, message_done = get_prpc_message

        mock_client_for_get_result.search_message_in_feedback.return_value = message_done

        with patch("prpc.app_client.ClientForSendMessage.get_instance", return_value=mock_client_for_send_message):
            task = AwaitableTask(message.func_name, message.func_args, message.func_kwargs)
            task.is_done_task(update_status=True)
            task.is_done_task(update_status=True)
            mock_client_for_send_message.client_broker_class.assert_called_once()

            mock_client_for_get_result.search_message_in_feedback.assert_called_once()
            mock_client_for_get_result.close.assert_called_once()
            task.get_result()

    def test_update_task_none(self, get_mocks, get_prpc_message):
        mock_client_for_get_result, mock_client_for_send_message = get_mocks
        message, _ = get_prpc_message

        with patch("prpc.app_client.ClientForSendMessage.get_instance", return_value=mock_client_for_send_message):
            task = AwaitableTask(message.func_name, message.func_args, message.func_kwargs)
            task.is_done_task(update_status=True)
            task.is_done_task(update_status=True)
            mock_client_for_send_message.client_broker_class.assert_called_once()

            assert mock_client_for_get_result.search_message_in_feedback.call_count == 2
            mock_client_for_get_result.close.assert_not_called()

    def test_timeout(self, get_mocks, get_prpc_message):
        mock_client_for_get_result, mock_client_for_send_message = get_mocks
        message, _ = get_prpc_message

        with patch("prpc.app_client.ClientForSendMessage.get_instance", return_value=mock_client_for_send_message):
            task = AwaitableTask(message.func_name, message.func_args, message.func_kwargs)
            with pytest.raises(Exception) as e:
                task.sync_wait_result_task(datetime.timedelta(seconds=1))

    def test_get_result_with_exception(self, get_mocks):
        mock_client_for_get_result, mock_client_for_send_message = get_mocks

        message = PRPCMessage("foo", (), {})
        message_done = copy(message)
        message_done.message_to_done("any exception")

        mock_client_for_get_result.search_message_in_feedback.return_value = message_done

        with patch("prpc.app_client.ClientForSendMessage.get_instance", return_value=mock_client_for_send_message):
            task = AwaitableTask(message.func_name, message.func_args, message.func_kwargs)
            with pytest.raises(Exception) as e:
                task.sync_wait_result_task()

            assert "any exception" == e.value.args[0]
