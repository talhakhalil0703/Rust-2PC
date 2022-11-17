#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command};
use client::Client;
use coordinator::ChildData;
use coordinator::Coordinator;
use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;
use participant::Participant;

///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions, tx: Sender<ProtocolMessage>) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    // This function must be used in conjunction with connect_to_coordinator, you're given the options of the child node which
    // includes the ipc name for which you need to connect to. This function is being called by the coordinator

    let (server, name) : (IpcOneShotServer<(Sender<ProtocolMessage>, String)>, String) = IpcOneShotServer::new().unwrap();

    child_opts.ipc_path = name; //Server name

    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");


    let (_, (send_message_to_child, child_server_name)) : (_, (Sender<ProtocolMessage>, String)) = server.accept().unwrap();

    // I now have the child_transmitter_channel to communicate with the child and send him stuff
    // I want to recieve stuff now
    let child_transmitter: Sender<Sender<ProtocolMessage>> = Sender::connect(child_server_name.to_string()).unwrap(); // Connecting to server
    child_transmitter.send(tx).unwrap();

    (child, send_message_to_child, rx)
}

///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (tx, rx) = channel().unwrap();

    //Connect to the server, and you should get Sender type
    let parent_transmitter: Sender<(Sender<ProtocolMessage>, String)> = Sender::connect(opts.ipc_path.to_string()).unwrap(); // Connecting to server

    //Setup Oneshot Server to pass own transmitter back to parent
    let (server, server_name) : (IpcOneShotServer<Sender<ProtocolMessage>>, String) = IpcOneShotServer::new().unwrap();

    parent_transmitter.send((tx, server_name)).unwrap(); // Sending this processes own transmitter through

    //Recieve transmitter from parent
    let (_, send_message_to_parent) = server.accept().unwrap();

    (send_message_to_parent, rx)
}

///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // 1. Creates a new coordinator
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");

    //Parents communication channels
    let (tx, rx) = channel().unwrap();

    let mut coordinator = Coordinator::new(coord_log_path, &running, rx, opts.num_requests);

    // 2. Spawns and connects to new clients processes and then registers them with
    //    the coordinator
    for n in 0..opts.num_clients{
        // The name of the client can be its server name
        let mut options = opts.clone();
        options.num = n;
        let (ch, trans, recv) = spawn_child_and_connect( &mut options, tx.clone());
        coordinator.client_join(ChildData{child:ch, send_to_child:trans, receive_from_child:recv});
    }

    // 3. Spawns and connects to new participant processes and then registers them
    //    with the coordinator
    for n in 0..opts.num_participants{
        // The name of the client can be its server name
        let mut options = opts.clone();
        options.num = n;
        let (ch, trans, recv) = spawn_child_and_connect( &mut options, tx.clone());
        coordinator.participant_join(ChildData{child:ch, send_to_child:trans, receive_from_child:recv});
    }


    // 4. Starts the coordinator protocol
    coordinator.protocol();

    // 5. Wait until the children finish execution
    // TODO
}

///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // 1. Connects to the coordinator to get tx/rx
    let (trans, recv) = connect_to_coordinator(opts);
    // 2. Constructs a new client
    let client_id_str = format!("client_{}", opts.num);
    let mut client = Client::new(client_id_str, &running, trans, recv);
    // 3. Starts the client protocol
    client.protocol(opts.num_requests);
}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);
    let (trans, recv) = connect_to_coordinator(opts);
    let mut participant = Participant::new(participant_id_str, participant_log_path,&running, opts.send_success_probability, opts.operation_success_probability,trans, recv);
    participant.protocol();
}

fn main() {
    // Parse CLI arguments
    let opts = tpcoptions::TPCOptions::new();
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}
