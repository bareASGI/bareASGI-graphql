"""GraphQL Base WebSocket instance"""

from abc import ABCMeta, abstractmethod
import asyncio
from asyncio import Future, Task
import json
import logging
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
    cast,
)

from bareasgi import WebSocket
import graphql
from graphql import ExecutionResult, GraphQLError, MapAsyncIterator

from .utils import has_subscription

LOGGER = logging.getLogger(__name__)

WS_INTERNAL_ERROR = 1011
WS_PROTOCOL = "graphql-ws"

GQL_CONNECTION_INIT = "connection_init"  # Client -> Server
GQL_CONNECTION_ACK = "connection_ack"  # Server -> Client
GQL_CONNECTION_ERROR = "connection_error"  # Server -> Client
GQL_CONNECTION_KEEP_ALIVE = "ka"  # Server -> Client
GQL_CONNECTION_TERMINATE = "connection_terminate"  # Client -> Server
GQL_START = "start"  # Client -> Server
GQL_DATA = "data"  # Server -> Client
GQL_ERROR = "error"  # Server -> Client
GQL_COMPLETE = "complete"  # Server -> Client
GQL_STOP = "stop"  # Client -> Server


class ProtocolError(Exception):
    """A protocol error"""


type Id = str | int


class GraphQLWebSocketHandlerInstanceBase(metaclass=ABCMeta):
    """A GraphQL WebSocket handler instance"""

    def __init__(self, web_socket: WebSocket, dumps: Callable[[Any], str]) -> None:
        self.web_socket = web_socket
        self._subscriptions: MutableMapping[Id, Future] = {}
        self._is_closed = False
        self.dumps = dumps

    async def start(self, subprotocols: Iterable[str]) -> None:
        """Start the WebSocket connection

        Args:
            subprotocols (Iterable[str]): Optional sub protocols

        Raises:
            ProtocolError: If the protocol is not supported
        """
        if WS_PROTOCOL not in subprotocols:
            raise ProtocolError(f"Expected subprotocol '{WS_PROTOCOL}")
        await self.web_socket.accept(WS_PROTOCOL)

        _type = GQL_CONNECTION_KEEP_ALIVE

        read_task: Task | None = None
        pending: set[Future] = set()

        while not (self._is_closed or _type in (GQL_CONNECTION_ERROR, GQL_CONNECTION_TERMINATE)):

            # We need to wait for the websocket and the subscriptions.
            if read_task is None or read_task not in pending:
                read_task = asyncio.create_task(self._read_message())
                pending.add(read_task)
            for task in self._subscriptions.values():
                if task not in pending:
                    pending.add(task)

            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                if task == read_task:
                    try:
                        type_, id_, payload = task.result()
                        await self._on_message(type_, id_, payload)
                        read_task = None
                    except EOFError:
                        self._is_closed = True
                        for pending_task in pending:
                            await self._stop_subscription(pending_task)
                            self._remove_subscription(pending_task)
                else:
                    # Subscription tasks are done when they complete or are cancelled.
                    self._remove_subscription(task)

        await self._unsubscribe_all()
        if not self._is_closed:
            await self.web_socket.close()

    async def _read_message(self) -> tuple[str, Id | None, dict | None]:
        text = await self.web_socket.receive()
        if text is None:
            raise EOFError

        if not isinstance(text, str):
            raise ProtocolError('Expected the message to be a string.')

        message: Mapping[str, Any] = json.loads(text)
        if not isinstance(message, dict):
            raise ProtocolError('Expected the message to be an object.')

        type_: str | None = message['type']
        if not isinstance(type_, str):
            raise ProtocolError("Expected field 'type' to be a string")

        id_: Id | None = message.get('id')
        if not (id_ is None or isinstance(id_, str) or isinstance(id_, int)):
            raise ProtocolError("Expected field 'id' to be an string?")

        payload = message.get('payload')
        if not (payload is None or isinstance(payload, dict)):
            raise ProtocolError("Expected field 'payload' to be an object?")

        return type_, id_, payload

    async def _on_message(self, type_: str, id_: Id | None, payload: dict | None) -> None:

        if type_ == GQL_CONNECTION_INIT:
            await self._on_connection_init(id_, payload)
        elif type_ == GQL_CONNECTION_TERMINATE:
            await self._on_connection_terminate()
        elif type_ == GQL_START:
            await self._on_start(id_, payload)
        elif type_ == GQL_STOP:
            if id_ is None:
                raise ProtocolError("Can't stop a subscription with no index")
            await self._on_stop(id_)
        elif type_ == GQL_CONNECTION_KEEP_ALIVE:
            pass
        else:
            raise ProtocolError(f"Received unknown message type '{type_}'.")

    async def _on_connection_init(self, id_: Id | None, _connection_params: Any | None) -> None:
        try:
            await self.web_socket.send(self._to_message('connection_ack', id_))
        except Exception as error:
            await self._send_error(GQL_CONNECTION_ERROR, id_, error)
            await self.web_socket.close(WS_INTERNAL_ERROR)
            raise

    async def _on_connection_terminate(self) -> None:
        await self.web_socket.close(WS_INTERNAL_ERROR)

    @abstractmethod
    async def subscribe(
            self,
            query: str,
            variables: dict[str, Any],
            operation_name: str | None
    ) -> MapAsyncIterator:
        """Execute a subscription.

        Args:
            query (str): The subscription query.
            variables (dict[str, Any]): Optional variables.
            operation_name (str | None): An optional operation name.

        Returns:
            MapAsyncIterator: An asynchronous iterator of the results.
        """

    @abstractmethod
    async def query(
            self,
            query: str,
            variables: dict[str, Any],
            operation_name: str | None
    ) -> ExecutionResult:
        """Execute a query

        Args:
            query (str): The subscription query.
            variables (dict[str, Any]): Optional variables.
            operation_name (str | None): An optional operation name.

        Returns:
            ExecutionResult: The query results.
        """

    async def _on_start(self, id_: Id | None, payload: list | dict | None) -> None:
        try:
            # An id is required for a start operation.
            if id_ is None:
                raise ProtocolError("required 'id' field must be an int.")

            if id_ in self._subscriptions:
                await self._unsubscribe(id_)
                del self._subscriptions[id_]

            query, variable_values, operation_name = self._parse_start_payload(
                payload)

            document = graphql.parse(query)
            # noinspection PyUnresolvedReferences
            if has_subscription(document):
                result: MapAsyncIterator | ExecutionResult = await self.subscribe(
                    query,
                    variable_values,
                    operation_name
                )
            else:
                result = await self.query(
                    query,
                    variable_values,
                    operation_name
                )

            if isinstance(result, ExecutionResult):
                await self._send_execution_result(id_, result)
                return

            self._add_subscription(id_, result)

        except Exception as error:  # pylint: disable=broad-except
            await self._send_error(GQL_ERROR, id_, error)

    def _add_subscription(self, id_: Id, result: AsyncIterator) -> None:
        self._subscriptions[id_] = asyncio.create_task(
            self._process_subscription(id_, result)
        )

    def _remove_subscription(self, future: Future) -> None:
        id_ = next(k for k, v in self._subscriptions.items() if v == future)
        del self._subscriptions[id_]

    async def _process_subscription(self, id_: Id, result: AsyncIterator) -> AsyncIterator:
        try:
            async for val in result:
                await self._send_execution_result(id_, val)
            await self.web_socket.send(self._to_message(GQL_COMPLETE, id_))
        except asyncio.CancelledError:
            pass
        except Exception as error:  # pylint: disable=broad-except
            if not isinstance(error, GraphQLError):
                error = GraphQLError('Execution error', original_error=error)
            await self._send_execution_result(
                id_,
                ExecutionResult(errors=[error])
            )
            await self.web_socket.send(self._to_message(GQL_COMPLETE, id_))

        return result

    async def _on_stop(self, id_: Id) -> None:
        await self._unsubscribe(id_)

    @classmethod
    async def _stop_subscription(cls, future: Future) -> None:
        future.cancel()
        await future
        result = future.result()
        await result.aclose()

    async def _unsubscribe(self, id_: Id) -> None:
        await self._stop_subscription(self._subscriptions[id_])

    async def _unsubscribe_all(self) -> None:
        # pylint: disable=consider-iterating-dictionary
        for id_ in self._subscriptions.keys():
            await self._unsubscribe(id_)

    async def _send_error(self, type_: str, id_: Id | None, error: Exception) -> None:
        await self.web_socket.send(self._to_message(type_, id_, {'message': str(error)}))

    async def _send_execution_result(
            self,
            id_: Id,
            execution_result: ExecutionResult
    ) -> None:
        result: dict[str, dict[str, Any] | list[Any]] = {}

        if execution_result.data:
            result["data"] = execution_result.data

        if execution_result.errors:
            result["errors"] = [
                error.formatted
                for error in execution_result.errors
            ]

        await self.web_socket.send(self._to_message(GQL_DATA, id_, result))

    def _to_message(
            self,
            type_: str,
            id_: Id | None = None,
            payload: Any | None = None
    ) -> str:
        message: dict[str, Any] = {'type': type_}
        if id_ is not None:
            message['id'] = id_
        if payload is not None:
            message['payload'] = payload
        return self.dumps(message)

    @classmethod
    def _parse_start_payload(
            cls,
            payload: dict | list | None
    ) -> tuple[str, dict[str, Any], str | None]:

        if not isinstance(payload, dict):
            raise ProtocolError("required 'payload' field must be an object.")

        query = payload.get('query')
        if not isinstance(query, str):
            raise ProtocolError(
                "required 'query' field must be string in 'payload'.")

        variable_values = payload.get('variables')
        if not (variable_values is None or isinstance(variable_values, dict)):
            raise ProtocolError(
                "optional 'variables' field must be object? in 'payload'.")

        operation_name = payload.get('operationName')
        if not (operation_name is None or isinstance(operation_name, str)):
            raise ProtocolError(
                "optional 'operationName' field must be str? in 'payload'.")

        return query, cast(dict[str, Any], variable_values), operation_name
