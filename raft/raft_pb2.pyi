from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RequestVoteRequest(_message.Message):
    __slots__ = ("candidateId", "term", "lastLogIndex", "lastLogTerm")
    CANDIDATEID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    LASTLOGINDEX_FIELD_NUMBER: _ClassVar[int]
    LASTLOGTERM_FIELD_NUMBER: _ClassVar[int]
    candidateId: str
    term: int
    lastLogIndex: int
    lastLogTerm: int
    def __init__(self, candidateId: _Optional[str] = ..., term: _Optional[int] = ..., lastLogIndex: _Optional[int] = ..., lastLogTerm: _Optional[int] = ...) -> None: ...

class RequestVoteResponse(_message.Message):
    __slots__ = ("term", "voteGranted")
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTEGRANTED_FIELD_NUMBER: _ClassVar[int]
    term: int
    voteGranted: bool
    def __init__(self, term: _Optional[int] = ..., voteGranted: bool = ...) -> None: ...

class LogEntry(_message.Message):
    __slots__ = ("command", "term")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    command: str
    term: int
    def __init__(self, command: _Optional[str] = ..., term: _Optional[int] = ...) -> None: ...

class AppendEntriesRequest(_message.Message):
    __slots__ = ("leaderId", "term", "prevLogIndex", "prevLogTerm", "leaderCommit", "entries")
    LEADERID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    PREVLOGINDEX_FIELD_NUMBER: _ClassVar[int]
    PREVLOGTERM_FIELD_NUMBER: _ClassVar[int]
    LEADERCOMMIT_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    leaderId: str
    term: int
    prevLogIndex: int
    prevLogTerm: int
    leaderCommit: int
    entries: _containers.RepeatedCompositeFieldContainer[LogEntry]
    def __init__(self, leaderId: _Optional[str] = ..., term: _Optional[int] = ..., prevLogIndex: _Optional[int] = ..., prevLogTerm: _Optional[int] = ..., leaderCommit: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[LogEntry, _Mapping]]] = ...) -> None: ...

class AppendEntriesResponse(_message.Message):
    __slots__ = ("term", "success", "ack")
    TERM_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ACK_FIELD_NUMBER: _ClassVar[int]
    term: int
    success: bool
    ack: int
    def __init__(self, term: _Optional[int] = ..., success: bool = ..., ack: _Optional[int] = ...) -> None: ...

class ServeClientRequest(_message.Message):
    __slots__ = ("request",)
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    request: str
    def __init__(self, request: _Optional[str] = ...) -> None: ...

class ServeClientResponse(_message.Message):
    __slots__ = ("data", "leaderId", "success")
    DATA_FIELD_NUMBER: _ClassVar[int]
    LEADERID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    data: str
    leaderId: str
    success: bool
    def __init__(self, data: _Optional[str] = ..., leaderId: _Optional[str] = ..., success: bool = ...) -> None: ...
