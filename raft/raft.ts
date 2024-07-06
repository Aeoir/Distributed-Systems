/* eslint-disable */
import { ChannelCredentials, Client, makeGenericClientConstructor, Metadata } from "@grpc/grpc-js";
import type {
  CallOptions,
  ClientOptions,
  ClientUnaryCall,
  handleUnaryCall,
  ServiceError,
  UntypedServiceImplementation,
} from "@grpc/grpc-js";
import _m0 from "protobufjs/minimal";

export const protobufPackage = "";

export interface RequestVoteRequest {
  candidateId: string;
  term: number;
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteResponse {
  term: number;
  voteGranted: boolean;
}

export interface LogEntry {
  command: string;
  term: number;
}

export interface AppendEntriesRequest {
  leaderId: string;
  term: number;
  prevLogIndex: number;
  prevLogTerm: number;
  leaderCommit: number;
  entries: LogEntry[];
}

export interface AppendEntriesResponse {
  term: number;
  success: boolean;
  ack: number;
}

export interface ServeClientRequest {
  request: string;
}

export interface ServeClientResponse {
  data: string;
  leaderId: string;
  success: boolean;
}

function createBaseRequestVoteRequest(): RequestVoteRequest {
  return { candidateId: "", term: 0, lastLogIndex: 0, lastLogTerm: 0 };
}

export const RequestVoteRequest = {
  encode(message: RequestVoteRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.candidateId !== "") {
      writer.uint32(10).string(message.candidateId);
    }
    if (message.term !== 0) {
      writer.uint32(16).int32(message.term);
    }
    if (message.lastLogIndex !== 0) {
      writer.uint32(24).int32(message.lastLogIndex);
    }
    if (message.lastLogTerm !== 0) {
      writer.uint32(32).int32(message.lastLogTerm);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RequestVoteRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRequestVoteRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.candidateId = reader.string();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.term = reader.int32();
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.lastLogIndex = reader.int32();
          continue;
        case 4:
          if (tag !== 32) {
            break;
          }

          message.lastLogTerm = reader.int32();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): RequestVoteRequest {
    return {
      candidateId: isSet(object.candidateId) ? globalThis.String(object.candidateId) : "",
      term: isSet(object.term) ? globalThis.Number(object.term) : 0,
      lastLogIndex: isSet(object.lastLogIndex) ? globalThis.Number(object.lastLogIndex) : 0,
      lastLogTerm: isSet(object.lastLogTerm) ? globalThis.Number(object.lastLogTerm) : 0,
    };
  },

  toJSON(message: RequestVoteRequest): unknown {
    const obj: any = {};
    if (message.candidateId !== "") {
      obj.candidateId = message.candidateId;
    }
    if (message.term !== 0) {
      obj.term = Math.round(message.term);
    }
    if (message.lastLogIndex !== 0) {
      obj.lastLogIndex = Math.round(message.lastLogIndex);
    }
    if (message.lastLogTerm !== 0) {
      obj.lastLogTerm = Math.round(message.lastLogTerm);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<RequestVoteRequest>, I>>(base?: I): RequestVoteRequest {
    return RequestVoteRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<RequestVoteRequest>, I>>(object: I): RequestVoteRequest {
    const message = createBaseRequestVoteRequest();
    message.candidateId = object.candidateId ?? "";
    message.term = object.term ?? 0;
    message.lastLogIndex = object.lastLogIndex ?? 0;
    message.lastLogTerm = object.lastLogTerm ?? 0;
    return message;
  },
};

function createBaseRequestVoteResponse(): RequestVoteResponse {
  return { term: 0, voteGranted: false };
}

export const RequestVoteResponse = {
  encode(message: RequestVoteResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.term !== 0) {
      writer.uint32(8).int32(message.term);
    }
    if (message.voteGranted !== false) {
      writer.uint32(16).bool(message.voteGranted);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RequestVoteResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRequestVoteResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.term = reader.int32();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.voteGranted = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): RequestVoteResponse {
    return {
      term: isSet(object.term) ? globalThis.Number(object.term) : 0,
      voteGranted: isSet(object.voteGranted) ? globalThis.Boolean(object.voteGranted) : false,
    };
  },

  toJSON(message: RequestVoteResponse): unknown {
    const obj: any = {};
    if (message.term !== 0) {
      obj.term = Math.round(message.term);
    }
    if (message.voteGranted !== false) {
      obj.voteGranted = message.voteGranted;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<RequestVoteResponse>, I>>(base?: I): RequestVoteResponse {
    return RequestVoteResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<RequestVoteResponse>, I>>(object: I): RequestVoteResponse {
    const message = createBaseRequestVoteResponse();
    message.term = object.term ?? 0;
    message.voteGranted = object.voteGranted ?? false;
    return message;
  },
};

function createBaseLogEntry(): LogEntry {
  return { command: "", term: 0 };
}

export const LogEntry = {
  encode(message: LogEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.command !== "") {
      writer.uint32(10).string(message.command);
    }
    if (message.term !== 0) {
      writer.uint32(16).int32(message.term);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): LogEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseLogEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.command = reader.string();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.term = reader.int32();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): LogEntry {
    return {
      command: isSet(object.command) ? globalThis.String(object.command) : "",
      term: isSet(object.term) ? globalThis.Number(object.term) : 0,
    };
  },

  toJSON(message: LogEntry): unknown {
    const obj: any = {};
    if (message.command !== "") {
      obj.command = message.command;
    }
    if (message.term !== 0) {
      obj.term = Math.round(message.term);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<LogEntry>, I>>(base?: I): LogEntry {
    return LogEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<LogEntry>, I>>(object: I): LogEntry {
    const message = createBaseLogEntry();
    message.command = object.command ?? "";
    message.term = object.term ?? 0;
    return message;
  },
};

function createBaseAppendEntriesRequest(): AppendEntriesRequest {
  return { leaderId: "", term: 0, prevLogIndex: 0, prevLogTerm: 0, leaderCommit: 0, entries: [] };
}

export const AppendEntriesRequest = {
  encode(message: AppendEntriesRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.leaderId !== "") {
      writer.uint32(10).string(message.leaderId);
    }
    if (message.term !== 0) {
      writer.uint32(16).int32(message.term);
    }
    if (message.prevLogIndex !== 0) {
      writer.uint32(24).int32(message.prevLogIndex);
    }
    if (message.prevLogTerm !== 0) {
      writer.uint32(32).int32(message.prevLogTerm);
    }
    if (message.leaderCommit !== 0) {
      writer.uint32(40).int32(message.leaderCommit);
    }
    for (const v of message.entries) {
      LogEntry.encode(v!, writer.uint32(50).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AppendEntriesRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAppendEntriesRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.leaderId = reader.string();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.term = reader.int32();
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.prevLogIndex = reader.int32();
          continue;
        case 4:
          if (tag !== 32) {
            break;
          }

          message.prevLogTerm = reader.int32();
          continue;
        case 5:
          if (tag !== 40) {
            break;
          }

          message.leaderCommit = reader.int32();
          continue;
        case 6:
          if (tag !== 50) {
            break;
          }

          message.entries.push(LogEntry.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): AppendEntriesRequest {
    return {
      leaderId: isSet(object.leaderId) ? globalThis.String(object.leaderId) : "",
      term: isSet(object.term) ? globalThis.Number(object.term) : 0,
      prevLogIndex: isSet(object.prevLogIndex) ? globalThis.Number(object.prevLogIndex) : 0,
      prevLogTerm: isSet(object.prevLogTerm) ? globalThis.Number(object.prevLogTerm) : 0,
      leaderCommit: isSet(object.leaderCommit) ? globalThis.Number(object.leaderCommit) : 0,
      entries: globalThis.Array.isArray(object?.entries) ? object.entries.map((e: any) => LogEntry.fromJSON(e)) : [],
    };
  },

  toJSON(message: AppendEntriesRequest): unknown {
    const obj: any = {};
    if (message.leaderId !== "") {
      obj.leaderId = message.leaderId;
    }
    if (message.term !== 0) {
      obj.term = Math.round(message.term);
    }
    if (message.prevLogIndex !== 0) {
      obj.prevLogIndex = Math.round(message.prevLogIndex);
    }
    if (message.prevLogTerm !== 0) {
      obj.prevLogTerm = Math.round(message.prevLogTerm);
    }
    if (message.leaderCommit !== 0) {
      obj.leaderCommit = Math.round(message.leaderCommit);
    }
    if (message.entries?.length) {
      obj.entries = message.entries.map((e) => LogEntry.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<AppendEntriesRequest>, I>>(base?: I): AppendEntriesRequest {
    return AppendEntriesRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<AppendEntriesRequest>, I>>(object: I): AppendEntriesRequest {
    const message = createBaseAppendEntriesRequest();
    message.leaderId = object.leaderId ?? "";
    message.term = object.term ?? 0;
    message.prevLogIndex = object.prevLogIndex ?? 0;
    message.prevLogTerm = object.prevLogTerm ?? 0;
    message.leaderCommit = object.leaderCommit ?? 0;
    message.entries = object.entries?.map((e) => LogEntry.fromPartial(e)) || [];
    return message;
  },
};

function createBaseAppendEntriesResponse(): AppendEntriesResponse {
  return { term: 0, success: false, ack: 0 };
}

export const AppendEntriesResponse = {
  encode(message: AppendEntriesResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.term !== 0) {
      writer.uint32(8).int32(message.term);
    }
    if (message.success !== false) {
      writer.uint32(16).bool(message.success);
    }
    if (message.ack !== 0) {
      writer.uint32(24).int32(message.ack);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AppendEntriesResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAppendEntriesResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.term = reader.int32();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.success = reader.bool();
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.ack = reader.int32();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): AppendEntriesResponse {
    return {
      term: isSet(object.term) ? globalThis.Number(object.term) : 0,
      success: isSet(object.success) ? globalThis.Boolean(object.success) : false,
      ack: isSet(object.ack) ? globalThis.Number(object.ack) : 0,
    };
  },

  toJSON(message: AppendEntriesResponse): unknown {
    const obj: any = {};
    if (message.term !== 0) {
      obj.term = Math.round(message.term);
    }
    if (message.success !== false) {
      obj.success = message.success;
    }
    if (message.ack !== 0) {
      obj.ack = Math.round(message.ack);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<AppendEntriesResponse>, I>>(base?: I): AppendEntriesResponse {
    return AppendEntriesResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<AppendEntriesResponse>, I>>(object: I): AppendEntriesResponse {
    const message = createBaseAppendEntriesResponse();
    message.term = object.term ?? 0;
    message.success = object.success ?? false;
    message.ack = object.ack ?? 0;
    return message;
  },
};

function createBaseServeClientRequest(): ServeClientRequest {
  return { request: "" };
}

export const ServeClientRequest = {
  encode(message: ServeClientRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.request !== "") {
      writer.uint32(10).string(message.request);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ServeClientRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseServeClientRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.request = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): ServeClientRequest {
    return { request: isSet(object.request) ? globalThis.String(object.request) : "" };
  },

  toJSON(message: ServeClientRequest): unknown {
    const obj: any = {};
    if (message.request !== "") {
      obj.request = message.request;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<ServeClientRequest>, I>>(base?: I): ServeClientRequest {
    return ServeClientRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ServeClientRequest>, I>>(object: I): ServeClientRequest {
    const message = createBaseServeClientRequest();
    message.request = object.request ?? "";
    return message;
  },
};

function createBaseServeClientResponse(): ServeClientResponse {
  return { data: "", leaderId: "", success: false };
}

export const ServeClientResponse = {
  encode(message: ServeClientResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.data !== "") {
      writer.uint32(10).string(message.data);
    }
    if (message.leaderId !== "") {
      writer.uint32(18).string(message.leaderId);
    }
    if (message.success !== false) {
      writer.uint32(24).bool(message.success);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ServeClientResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseServeClientResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.data = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.leaderId = reader.string();
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.success = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): ServeClientResponse {
    return {
      data: isSet(object.data) ? globalThis.String(object.data) : "",
      leaderId: isSet(object.leaderId) ? globalThis.String(object.leaderId) : "",
      success: isSet(object.success) ? globalThis.Boolean(object.success) : false,
    };
  },

  toJSON(message: ServeClientResponse): unknown {
    const obj: any = {};
    if (message.data !== "") {
      obj.data = message.data;
    }
    if (message.leaderId !== "") {
      obj.leaderId = message.leaderId;
    }
    if (message.success !== false) {
      obj.success = message.success;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<ServeClientResponse>, I>>(base?: I): ServeClientResponse {
    return ServeClientResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ServeClientResponse>, I>>(object: I): ServeClientResponse {
    const message = createBaseServeClientResponse();
    message.data = object.data ?? "";
    message.leaderId = object.leaderId ?? "";
    message.success = object.success ?? false;
    return message;
  },
};

export type RaftService = typeof RaftService;
export const RaftService = {
  requestVote: {
    path: "/Raft/RequestVote",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: RequestVoteRequest) => Buffer.from(RequestVoteRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => RequestVoteRequest.decode(value),
    responseSerialize: (value: RequestVoteResponse) => Buffer.from(RequestVoteResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => RequestVoteResponse.decode(value),
  },
  appendEntries: {
    path: "/Raft/AppendEntries",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: AppendEntriesRequest) => Buffer.from(AppendEntriesRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => AppendEntriesRequest.decode(value),
    responseSerialize: (value: AppendEntriesResponse) => Buffer.from(AppendEntriesResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => AppendEntriesResponse.decode(value),
  },
  serveClient: {
    path: "/Raft/ServeClient",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: ServeClientRequest) => Buffer.from(ServeClientRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => ServeClientRequest.decode(value),
    responseSerialize: (value: ServeClientResponse) => Buffer.from(ServeClientResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => ServeClientResponse.decode(value),
  },
} as const;

export interface RaftServer extends UntypedServiceImplementation {
  requestVote: handleUnaryCall<RequestVoteRequest, RequestVoteResponse>;
  appendEntries: handleUnaryCall<AppendEntriesRequest, AppendEntriesResponse>;
  serveClient: handleUnaryCall<ServeClientRequest, ServeClientResponse>;
}

export interface RaftClient extends Client {
  requestVote(
    request: RequestVoteRequest,
    callback: (error: ServiceError | null, response: RequestVoteResponse) => void,
  ): ClientUnaryCall;
  requestVote(
    request: RequestVoteRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: RequestVoteResponse) => void,
  ): ClientUnaryCall;
  requestVote(
    request: RequestVoteRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: RequestVoteResponse) => void,
  ): ClientUnaryCall;
  appendEntries(
    request: AppendEntriesRequest,
    callback: (error: ServiceError | null, response: AppendEntriesResponse) => void,
  ): ClientUnaryCall;
  appendEntries(
    request: AppendEntriesRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: AppendEntriesResponse) => void,
  ): ClientUnaryCall;
  appendEntries(
    request: AppendEntriesRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: AppendEntriesResponse) => void,
  ): ClientUnaryCall;
  serveClient(
    request: ServeClientRequest,
    callback: (error: ServiceError | null, response: ServeClientResponse) => void,
  ): ClientUnaryCall;
  serveClient(
    request: ServeClientRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: ServeClientResponse) => void,
  ): ClientUnaryCall;
  serveClient(
    request: ServeClientRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: ServeClientResponse) => void,
  ): ClientUnaryCall;
}

export const RaftClient = makeGenericClientConstructor(RaftService, "Raft") as unknown as {
  new (address: string, credentials: ChannelCredentials, options?: Partial<ClientOptions>): RaftClient;
  service: typeof RaftService;
  serviceName: string;
};

type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;

export type DeepPartial<T> = T extends Builtin ? T
  : T extends globalThis.Array<infer U> ? globalThis.Array<DeepPartial<U>>
  : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>>
  : T extends {} ? { [K in keyof T]?: DeepPartial<T[K]> }
  : Partial<T>;

type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P
  : P & { [K in keyof P]: Exact<P[K], I[K]> } & { [K in Exclude<keyof I, KeysOfUnion<P>>]: never };

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
