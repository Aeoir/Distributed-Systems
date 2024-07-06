import { Term, Role, Vote, NodeState, NodeId, LogEntry } from './type'
import dotenv from 'dotenv'
import { RequestVoteRequest, RaftClient, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, RaftService, ServeClientRequest, ServeClientResponse } from './raft'
import { Server, ServerCredentials, ServerUnaryCall, credentials, sendUnaryData } from "@grpc/grpc-js"
import fs from 'fs'

dotenv.config()

const SELFID = String(process.argv[2])
const NODES = String(process.env.NODES).split(',')
const SELFIDX = NODES.indexOf(SELFID)
const ELECTION_TIMEOUT = Number(process.argv[3])
const HEARTBEAT_FREQUENCY = Number(process.env.HEARTBEAT_FREQUENCY)
const LEASE_TIMEOUT = Number(process.env.LEASE_TIMEOUT)
const LOGPATH = 'logs_node_' + String(SELFIDX) 
const CACHE = new Map()

const GlobalState: NodeState = {
  currentTerm: 0,
  votedFor: null,
  log: [],
  commitLength: 0,
  currentRole: Role.FOLLOWER,
  currentLeader: null,
  votesReceived: new Set<Vote>(),
  heartbeatResponses: new Set<Vote>(),
  sentLength: new Array(NODES.length),
  ackedLength: new Array(NODES.length),
  electionTimer: undefined,
  leaseTimeout: false
}

const callClientCallback = (commitIndex: number, callback: sendUnaryData<ServeClientResponse>) => {
  let success = false
  if (GlobalState.commitLength >= commitIndex) {
    success = true
  }

  const res: ServeClientResponse = {
    data: '',
    leaderId: String(GlobalState.currentLeader),
    success: success
  }
  callback(null, res)
}

// Upon election start each node will send remaining lease timeout to candidate
// Candidate will fail the election if lease is pending
// Some other node will become candidate and win election after leases on all nodes expire
// After becoming leader, candidate will 

// TODO: on recovery from crash
// update GlobalState values from log files
if (fs.existsSync(LOGPATH) && fs.existsSync(LOGPATH + '/logs.txt')) {
  const content = fs.readFileSync(LOGPATH + '/logs.txt').toString()
  let logs = content.split('\n')
  logs = logs.slice(0, logs.length - 1)
  const restoredLogs = logs.map(log => {
    const values = log.split(' ')
    if (values[0] === 'NO-OP') {
      return {
        command: values[0],
        term: Number(values[1])
      }
    } else {
      return {
        command: values.slice(0, 3).join(' '),
        term: Number(values[3])
      }
    }
  })

  console.log('Restored logs: ', restoredLogs)

  GlobalState.log = restoredLogs
}

if (fs.existsSync(LOGPATH) && fs.existsSync(LOGPATH + '/metadata.txt')) {
  const content = fs.readFileSync(LOGPATH + '/metadata.txt').toString()
  const values = content.split('\n')
  GlobalState.commitLength = Number(values[0].split(' ')[2])
  GlobalState.currentTerm = Number(values[1].split(' ')[1])
  GlobalState.votedFor = values[2].split(' ')[2]

  console.log(`Restored metadata:\nCommit length: ${GlobalState.commitLength}\nTerm: ${GlobalState.currentTerm}\nVoted for: ${GlobalState.votedFor}`)
}

const dumpLogs = () => {
  try {
    if (!fs.existsSync(LOGPATH)) {
      fs.mkdirSync(LOGPATH);
    }

    const logs = GlobalState.log.reduce((prev, cur) => {
      return prev + cur.command + ' ' + String(cur.term) + '\n' 
    }, '')
    fs.writeFileSync(LOGPATH + '/logs.txt', logs)
  } catch (err) {
    console.log(err)
  }
}

const dumpMetadata = () => {
  try {
    if (!fs.existsSync(LOGPATH)) {
      fs.mkdirSync(LOGPATH);
    }

    const metadata = `Commit length: ${GlobalState.commitLength}\nTerm: ${GlobalState.currentTerm}\nVoted for: ${GlobalState.votedFor}\n` 
    fs.writeFileSync(LOGPATH + '/metadata.txt', metadata)
  } catch (err) {
    console.log(err)
  }
}

const cancelElectionTimer = () => {
  if (GlobalState.electionTimer != undefined) {
    clearTimeout(GlobalState.electionTimer)
    GlobalState.electionTimer = undefined
  }
}

const commitLogEntries = () => {
  while (GlobalState.commitLength < GlobalState.log.length) {
    let acks = 0
    for (let i = 0; i < NODES.length; i++) {
      if (GlobalState.ackedLength[i] > GlobalState.commitLength) {
        acks += 1
      }
    }
    if (acks >= Math.ceil((NODES.length + 1) / 2)) {
      // TODO: deliver log[commitLength].command to the application
      const command = GlobalState.log[GlobalState.commitLength].command.split(' ')
      const op = command[0]
      console.log(`Committing at ${SELFID}: ${op}`)
      if (op === 'SET') {
        CACHE.set(command[1], command[2])
      }
      GlobalState.commitLength += 1
      dumpMetadata()
    } else {
      break;
    }
  }
}

const appendEntries = (prefixLen: number, leaderCommit: number, suffix: LogEntry[]) => {
  if (suffix.length > 0 && GlobalState.log.length > prefixLen) {
    const index = Math.min(GlobalState.log.length, prefixLen + suffix.length) - 1
    if (GlobalState.log[index].term !== suffix[index - prefixLen].term) {
      GlobalState.log = GlobalState.log.slice(0, prefixLen /*-1*/)
    }
  }

  if (prefixLen + suffix.length > GlobalState.log.length) {
    for (let i = GlobalState.log.length - prefixLen; i < suffix.length; i++) {
      GlobalState.log.push(suffix[i])
    }
  }

  if (leaderCommit > GlobalState.commitLength) {
    for (let i = GlobalState.commitLength; i < leaderCommit; i++) {
      // TODO: deliver log[i].command to application
      const command = GlobalState.log[i].command.split(' ')
      const op = command[0]
      console.log(`Committing at ${SELFID}: ${op}`)
      if (op === 'SET') {
        CACHE.set(command[1], command[2])
      }
    }
    GlobalState.commitLength = leaderCommit
  }
  dumpLogs()
  dumpMetadata()
}

const logResponse = (follower: NodeId, term: Term, ack: number, success: boolean) => {
  if (term == GlobalState.currentTerm && GlobalState.currentRole == Role.LEADER) {
    const followerIdx = NODES.indexOf(follower)
    GlobalState.heartbeatResponses.add(follower)
    if (success == true && ack >= GlobalState.ackedLength[followerIdx]) {
      GlobalState.sentLength[followerIdx] = ack;
      GlobalState.ackedLength[followerIdx] = ack;
      commitLogEntries()
    } else if (GlobalState.sentLength[followerIdx] > 0) {
      GlobalState.sentLength[followerIdx] -= 1
      replicateLog(SELFID, follower);
    }
  } else if (term > GlobalState.currentTerm) {
    GlobalState.currentTerm = term
    GlobalState.currentRole = Role.FOLLOWER
    GlobalState.votedFor = null
    cancelElectionTimer()
    dumpMetadata()
  }
}

const replicateLog = (leaderId: NodeId, followerId: NodeId) => {
  const followerIdx = NODES.indexOf(followerId)
  const prefixLen = GlobalState.sentLength[followerIdx]
  const suffix = GlobalState.log.slice(prefixLen, GlobalState.log.length /*-1*/)
  let prefixTerm = 0

  if (prefixLen > 0) {
    prefixTerm = GlobalState.log[prefixLen - 1].term
  }

  const client = new RaftClient(
    followerId,
    credentials.createInsecure()
  )

  const req: AppendEntriesRequest = {
    leaderId,
    term: GlobalState.currentTerm,
    prevLogIndex: prefixLen,
    prevLogTerm: prefixTerm,
    leaderCommit: GlobalState.commitLength,
    entries: suffix
  }
  client.appendEntries(req, (err, res) => {
    if (err) {
      console.log('Failed to send AppendEntriesRPC to ' + followerId)
    } else {
      logResponse(followerId, res.term, res.ack, res.success)
    }
    client.close()
  })
}

const receiveVote = (voterId: NodeId, term: number, granted: boolean) => {
  console.log(`Recieved vote from Node: ${voterId} with term: ${term} and granted: ${granted}`)
  if (GlobalState.currentRole == Role.CANDIDATE && term == GlobalState.currentTerm && granted) {
    GlobalState.votesReceived.add(voterId)
    if (GlobalState.votesReceived.size >= Math.ceil((NODES.length + 1) / 2)) {
      GlobalState.currentRole = Role.LEADER
      GlobalState.currentLeader = SELFID
      cancelElectionTimer()
      console.log(`Became leader: ${SELFID}`)
      GlobalState.log.push({
        term: GlobalState.currentTerm,
        command: 'NO-OP'
      })
      dumpLogs()
      // start a timeout that checks for number of heartbeatResponses received in the lease interval and steps down if it is less than majority or restarts the timeout if ok
      GlobalState.leaseTimeout = true
      console.log('Acquiring Lease')
      setTimeout(() => {
        GlobalState.leaseTimeout = false
        if (GlobalState.heartbeatResponses.size < Math.ceil((NODES.length + 1) / 2)) {
          GlobalState.currentRole = Role.FOLLOWER
          GlobalState.votedFor = null
        }
        GlobalState.heartbeatResponses.clear()
        GlobalState.heartbeatResponses.add(SELFID)
      }, LEASE_TIMEOUT)
      for (const [i, follower] of NODES.entries()) {
        if (follower != SELFID) {
          GlobalState.sentLength[i] = GlobalState.log.length
          GlobalState.ackedLength[i] = 0
          replicateLog(SELFID, follower)
        }
      }
    } else if (term > GlobalState.currentTerm) {
      GlobalState.currentTerm = term
      GlobalState.currentRole = Role.FOLLOWER
      GlobalState.votedFor = null
      cancelElectionTimer()
      dumpMetadata()
    }
  }
}

const startElection = () => {
  console.log('Election timed out: becoming candidate')
  GlobalState.currentTerm += 1
  GlobalState.currentRole = Role.CANDIDATE
  GlobalState.votedFor = SELFID
  GlobalState.votesReceived.add(SELFID)
  dumpMetadata()

  let lastTerm = 0
  if (GlobalState.log.length > 0) {
    lastTerm = GlobalState.log[GlobalState.log.length - 1].term
  }

  for (const node of NODES) {
    if (node != SELFID) {
      const req: RequestVoteRequest = {
        candidateId: SELFID,
        term: GlobalState.currentTerm,
        lastLogIndex: GlobalState.log.length,
        lastLogTerm: lastTerm
      }

      const client = new RaftClient(
        node,
        credentials.createInsecure()
      )

      client.requestVote(req, (err, res) => {
        if (err) {
          console.log('Failed to send vote request to ' + node)
        } else {
          receiveVote(node, res.term, res.voteGranted)
        }
        client.close()
      })
    }
  }

  cancelElectionTimer()
}

setInterval(() => {
  if (GlobalState.electionTimer == undefined && GlobalState.currentRole !== Role.LEADER) {
    console.log('Restarting election timer')
    GlobalState.electionTimer = setTimeout(startElection, ELECTION_TIMEOUT)
  }
}, 50)

setInterval(() => {
  if (GlobalState.currentRole === Role.LEADER && !GlobalState.leaseTimeout) {
    GlobalState.leaseTimeout = true
    console.log('Acquiring Lease')
    setTimeout(() => {
      GlobalState.leaseTimeout = false
      if (GlobalState.heartbeatResponses.size < Math.ceil((NODES.length + 1) / 2)) {
        console.log('Failed to acquire lease. Stepping down.')
        GlobalState.currentRole = Role.FOLLOWER
        GlobalState.votedFor = null
      }
      GlobalState.heartbeatResponses.clear()
      GlobalState.heartbeatResponses.add(SELFID)
    }, LEASE_TIMEOUT)
  }
}, 50)

setInterval(() => {
  if (GlobalState.currentRole === Role.LEADER) {
    for (const follower of NODES) {
      if (follower != SELFID) {
        replicateLog(SELFID, follower)
      }
    }
  }
}, HEARTBEAT_FREQUENCY)


/* ----- SERVER ----- */
const requestVote = (
  call: ServerUnaryCall<RequestVoteRequest, RequestVoteResponse>,
  callback: sendUnaryData<RequestVoteResponse>
) => {
  const candidate = call.request
  console.log(`Received vote request from Node:${candidate.candidateId} with term: ${candidate.term}`)
  if (candidate.term > GlobalState.currentTerm) {
    GlobalState.currentTerm = candidate.term
    GlobalState.currentRole = Role.FOLLOWER
    GlobalState.votedFor = null
    dumpMetadata()
  }

  let lastTerm = 0
  
  if (GlobalState.log.length > 0) {
    lastTerm = GlobalState.log[GlobalState.log.length - 1].term
  }

  const logOk = (candidate.lastLogTerm > lastTerm) || (candidate.lastLogTerm == lastTerm &&
     candidate.lastLogIndex >= GlobalState.log.length)

  if (candidate.term == GlobalState.currentTerm && logOk && (GlobalState.votedFor == candidate.candidateId || GlobalState.votedFor == null) && !GlobalState.leaseTimeout) {
    GlobalState.votedFor = candidate.candidateId
    const res: RequestVoteResponse = {
      term: GlobalState.currentTerm,
      voteGranted: true
    }
    callback(null, res)
  } else {
    const res: RequestVoteResponse = {
      term: GlobalState.currentTerm,
      voteGranted: false
    }
    callback(null, res)
  }
}

const logRequest = (
  call: ServerUnaryCall<AppendEntriesRequest, AppendEntriesResponse>,
  callback: sendUnaryData<AppendEntriesResponse>
) => {
  const leader = call.request
  console.log(`Received AppendEntriesRPC from ${leader.leaderId} with term: ${leader.term}. Previous log index: ${leader.prevLogIndex} and entries[] size: ${leader.entries.length}`)
  if (leader.term > GlobalState.currentTerm) {
    GlobalState.currentTerm = leader.term
    GlobalState.votedFor = null
    dumpMetadata()
  }

  if (leader.term == GlobalState.currentTerm) {
    GlobalState.currentRole = Role.FOLLOWER
    GlobalState.currentLeader = leader.leaderId
    cancelElectionTimer()
    GlobalState.leaseTimeout = true
    setTimeout(() => {
      GlobalState.leaseTimeout = false
    }, LEASE_TIMEOUT)
  }

  const logOk = (GlobalState.log.length >= leader.prevLogIndex) && (leader.prevLogIndex == 0 || GlobalState.log[leader.prevLogIndex - 1].term == leader.prevLogTerm)

  if (leader.term == GlobalState.currentTerm && logOk) {
    appendEntries(leader.prevLogIndex, leader.leaderCommit, leader.entries)
    const ack = leader.prevLogIndex + leader.entries.length
    const res: AppendEntriesResponse = {
      term: GlobalState.currentTerm,
      ack,
      success: true
    }
    callback(null, res)
  } else {
    const res: AppendEntriesResponse = {
      term: GlobalState.currentTerm,
      ack: 0,
      success: false
    }
    callback(null, res)
  }
}

const serveClient = (
  call: ServerUnaryCall<ServeClientRequest, ServeClientResponse>,
  callback: sendUnaryData<ServeClientResponse>
) => {
  if (GlobalState.currentRole == Role.LEADER) {
    const request = call.request.request
    const op = request.split(' ')[0]
    if (op === 'SET') {
      GlobalState.log.push({
        term: GlobalState.currentTerm,
        command: call.request.request
      })
      dumpLogs()
      GlobalState.ackedLength[SELFIDX] = GlobalState.log.length

      setTimeout(() => {
        callClientCallback(GlobalState.log.length, callback)
      }, 2000)
      for (const follower of NODES) {
        if (follower != SELFID) {
          replicateLog(SELFID, follower)
        }
      }
    } else if (op === 'GET') {
      const key = request.split(' ')[1]
      const res: ServeClientResponse = {
        data: String(CACHE.get(key)),
        leaderId: String(GlobalState.currentLeader),
        success: true
      }

      callback(null, res)
    }
  } else {

    if (GlobalState.currentLeader == null) {
      const res: ServeClientResponse = {
        data: '',
        leaderId: String(GlobalState.currentLeader),
        success: false
      }
      callback(null, res)
      return
    }
    const client = new RaftClient(
      String(GlobalState.currentLeader),
      credentials.createInsecure()
    )
    client.serveClient(call.request, (err, res) => {
      if (err) {
        console.log(err)
      } else {
        callback(null, res)
      }
    })
  }
}

const server = new Server()
server.addService(RaftService, {
  requestVote,
  appendEntries: logRequest,
  serveClient
})
server.bindAsync(
  SELFID,
  ServerCredentials.createInsecure(),
  (error, port) => {
    if (error) {
      throw error;
    }
    console.log("server is running on", port);
  }
);
/* ----- SERVER ----- */
