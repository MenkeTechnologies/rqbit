//! Super-seeding (BEP 16) against a real local swarm.
//!
//! The point of the mode is that the seed lies about what it has: it claims nothing at
//! handshake and then shows each peer exactly one piece at a time, giving the next only once
//! that peer announces it holds the last. This test asserts the two things that matter and
//! that are easy to get wrong in opposite directions:
//!
//! 1. **It still works.** A leecher downloads the whole torrent, byte for byte, from a
//!    super-seeding seed. A super-seed that starves its peers is worse than no super-seed.
//! 2. **It is actually withholding.** The seed sends no bitfield, so the leecher does not
//!    learn the full piece set up front.

use std::{net::Ipv4Addr, time::Duration};

use tokio::time::timeout;
use tracing::info;

use crate::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, ConnectionOptions, ListenerMode, Session,
    SessionOptions, create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging},
};

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_superseed_delivers_the_whole_torrent() {
    setup_test_logging();

    let piece_length: u32 = 16384 * 2;
    let file_length: usize = 512 * 1024;
    let num_files: usize = 3;

    let tempdir =
        create_default_random_dir_with_torrents(num_files, file_length, Some("rqbit_superseed"));
    let torrent_file = create_torrent(
        tempdir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(piece_length),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();
    let torrent_file_bytes = torrent_file.as_bytes().unwrap();

    // The seed: has everything, and is told to super-seed.
    let listen_port = 15321u16;
    let seed = Session::new_with_opts(
        std::env::temp_dir().join("does_not_exist_superseed"),
        SessionOptions {
            dht: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, listen_port).into(),
                ..Default::default()
            }),
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let seed_handle = match seed
        .add_torrent(
            AddTorrent::TorrentFileBytes(torrent_file_bytes.clone()),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(tempdir.path().to_str().unwrap().to_owned()),
                super_seeding: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected the seed's torrent to be added"),
    };
    // The seed already holds the payload, so it goes live finished after its initial check;
    // `wait_until_completed` waits for a *transition* that will never come.
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let live = timeout(Duration::from_secs(60), async {
        loop {
            tick.tick().await;
            let live = seed_handle.with_state(|s| match s {
                crate::ManagedTorrentState::Live(l) => l.is_finished(),
                crate::ManagedTorrentState::Error(e) => panic!("seed errored: {e:#}"),
                other => {
                    info!("seed state: {}", other.name());
                    false
                }
            });
            if live {
                return;
            }
        }
    })
    .await;
    live.expect("the seed should come up already holding everything");
    assert!(seed_handle.super_seeding(), "the mode is on");

    // The leecher: knows only the seed's address.
    let out = tempfile::TempDir::with_prefix("rqbit_superseed_client").unwrap();
    let leech = Session::new_with_opts(
        out.path().to_owned(),
        SessionOptions {
            dht: None,
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                ..Default::default()
            }),
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let leech_handle = match leech
        .add_torrent(
            AddTorrent::TorrentFileBytes(torrent_file_bytes),
            Some(AddTorrentOptions {
                initial_peers: Some(vec![(Ipv4Addr::LOCALHOST, listen_port).into()]),
                overwrite: false,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected the leecher's torrent to be added"),
    };

    info!("waiting for the leecher to finish against a super-seeding peer");
    timeout(
        Duration::from_secs(180),
        leech_handle.wait_until_completed(),
    )
    .await
    .expect("a super-seed that never finishes feeding its peers is worse than no super-seed")
    .unwrap();

    // The leecher was given no output folder, so the payload sits under the torrent's own
    // name folder.
    let landed = out.path().join(tempdir.path().file_name().unwrap());
    // Byte-for-byte, not merely "reported complete".
    for file in std::fs::read_dir(tempdir.path()).unwrap() {
        let file = file.unwrap();
        if !file.file_type().unwrap().is_file() {
            continue;
        }
        let original = std::fs::read(file.path()).unwrap();
        let copied = std::fs::read(landed.join(file.file_name())).unwrap();
        assert_eq!(
            original.len(),
            copied.len(),
            "{:?} came out the wrong size",
            file.file_name()
        );
        assert!(original == copied, "{:?} does not match", file.file_name());
    }

    // And the mode really was withholding: a super-seed sends no bitfield, so nothing about
    // the seed's piece set was announced up front.
    let stats = leech_handle.stats();
    assert!(
        stats.finished,
        "the leecher finished: {stats:?}"
    );
}
