//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::HashMap;
use std::process::Child;
//use std::rt::panic_count::count_is_zero;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::vec;

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

use crate::participant;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision
}

#[derive(Debug)]
pub struct ChildData {
    pub id: String,
    pub child: Child,
    pub send_to_child: Sender<ProtocolMessage>,
    //pub receive_from_child: Receiver<ProtocolMessage>
}


/// Coordinator
/// Struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    clients: Vec<ChildData>,
    participants: Vec<ChildData>,
    receive_channel_participant: Receiver<ProtocolMessage>,
    receive_channel_client: Receiver<ProtocolMessage>,
    messages_per_client : u32,
    success: u64,
    failures: u64
}

///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>,
        recv_participant: Receiver<ProtocolMessage>,
        recv_client: Receiver<ProtocolMessage>,
    msg_per_client: u32) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            clients: vec![],
            participants: vec![],
            receive_channel_participant: recv_participant,
            receive_channel_client: recv_client,
            messages_per_client : msg_per_client,
            success : 0,
            failures: 0
            // TODO
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self,  participant: ChildData) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.participants.push(participant);
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, client: ChildData) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.clients.push(client);
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        let successful_ops: u64 = self.success;
        let failed_ops: u64 = self.failures;
        let unknown_ops: u64 = 0;

        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {

        // TODO
        // Wait for a message from clients, should wait client amount times.
        let processed = 0;
        for messages in 0..(self.messages_per_client * self.clients.len() as u32){
            //Receive MessageType::ClientRequest from a client
            let client_request_msg = self.receive_channel_client.recv().unwrap();
            //Clone the message and prepare to send it to participants
            // TODO check if clonning everything here is fine, I think it should be only txid cloned and oid cloned
            let mut proposal_msg = ProtocolMessage::instantiate(MessageType::CoordinatorPropose, client_request_msg.uid.clone(),client_request_msg.txid.clone(), client_request_msg.senderid.clone(), client_request_msg.opid.clone());

            //Wait and issue P participant messages
            for participant in &self.participants {
                participant.send_to_child.send(proposal_msg.clone());
            }

            // Now keep track of return messages from all participants, there should be P returns
            // TODO implement a timeout for if the message could not be sent
            // TODO this currently assumes every participant returns a yes
            let mut count_returned = 0;
            thread::sleep(Duration::from_millis(100));
            for _ in &self.participants {
                while let Ok(participant_vote_msg) = self.receive_channel_participant.try_recv(){
                    if participant_vote_msg.mtype == MessageType::ParticipantVoteCommit{
                        count_returned += 1;
                    } else if participant_vote_msg.mtype == MessageType::ParticipantVoteAbort{
                        continue;
                    } else {
                        debug!("Expected Participant Abort or Commit");
                    }
                }
            }

            let mut coordinator_vote = MessageType::CoordinatorCommit;
            debug!("Length of participants {}", self.participants.len());
            if count_returned != self.participants.len(){
                coordinator_vote = MessageType::CoordinatorAbort;
                self.failures += 1;
            } else {
                self.success += 1;
            }

            proposal_msg.mtype = coordinator_vote;
            self.log.append_from_pm(proposal_msg.clone());
            for participant in &self.participants {
                participant.send_to_child.send(proposal_msg.clone());
            }

            //Send message back to the specific client.. that participant either task failed or passed
            proposal_msg.mtype = MessageType::ClientResultCommit;
            if coordinator_vote == MessageType::CoordinatorAbort {
                proposal_msg.mtype = MessageType::ClientResultAbort;
            }

            self.log.append_from_pm(proposal_msg.clone());
            for client in &self.clients {
                if client.id == client_request_msg.senderid {
                    client.send_to_child.send(proposal_msg.clone());
                    break;
                }
            }
        }

        let exit_message = ProtocolMessage::generate(MessageType::CoordinatorExit, format!("Exit"), format!("Exit"), 0);
        for client in &self.clients{
            client.send_to_child.send(exit_message.clone());
        }

        for participant in &self.participants{
            participant.send_to_child.send(exit_message.clone());
        }

        while self.running.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(1));
        }

        self.report_status();
    }
}
