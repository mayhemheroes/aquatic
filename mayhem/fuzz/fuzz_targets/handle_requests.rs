// Mayhem fuzz target: handle_requests
//
// Honest reconstruction of the old `handle_requests` target. The original (in the pre-refactor
// fork at aquatic_udp/fuzz/fuzz_targets/handle_requests.rs) fed an `arbitrary`-derived
// `(Vec<(ConnectedRequest, CanonicalSocketAddr)>, u8)` into
// `aquatic_udp::workers::swarm::{handle_announce_request, handle_scrape_request}` against a
// `TorrentMaps`, mirroring `run_request_worker`.
//
// Current upstream (greatest-ape/aquatic) refactored that module: `ConnectedRequest`,
// `handle_announce_request`, and `handle_scrape_request` are gone. The honest successor is
// `aquatic_udp::swarm::TorrentMaps::{announce, scrape}` — the same in-tracker request handling
// logic, now driven directly from the parsed protocol requests. We mirror the exact call shape
// used by upstream's own integration test crates/udp/tests/cleaning.rs.
//
// To avoid depending on an `Arbitrary` impl for the (zerocopy, non-Arbitrary) protocol request
// types, we feed raw fuzzer bytes through the REAL wire parser
// (`aquatic_udp_protocol::Request::parse_bytes`) and route each parsed Announce/Scrape into the
// swarm handlers. This exercises both the parser and the real swarm peer-map / response logic
// (peer insertion, response peer selection, scrape stats), not a stub.
#![no_main]

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use aquatic_common::{CanonicalSocketAddr, SecondsSinceServerStart, ValidUntil};
use aquatic_udp::config::Config;
use aquatic_udp::swarm::TorrentMaps;
use aquatic_udp_protocol::Request;

use crossbeam_channel::unbounded;
use libfuzzer_sys::fuzz_target;
use rand::prelude::SmallRng;
use rand::SeedableRng;

fuzz_target!(|data: &[u8]| {
    let config = Config::default();

    // Mirrors aquatic_udp::workers::request worker setup (see crates/udp/tests/cleaning.rs).
    let torrents = TorrentMaps::default();
    let (statistics_sender, _statistics_receiver) = unbounded();
    let mut rng = SmallRng::seed_from_u64(0);
    let valid_until = ValidUntil::new_raw(SecondsSinceServerStart::new_raw(0));

    // Split the input into length-prefixed request frames. Each frame is parsed by the real wire
    // parser; valid Announce/Scrape requests are dispatched into the swarm with a per-frame source
    // address (alternating IPv4/IPv6 so both code paths are exercised).
    let mut cursor = data;
    let mut counter: u8 = 0;

    while cursor.len() >= 2 {
        // 2-byte big-endian length prefix selects how many bytes this frame consumes.
        let len = ((cursor[0] as usize) << 8 | cursor[1] as usize).min(cursor.len() - 2);
        let (frame, rest) = cursor[2..].split_at(len);
        cursor = rest;

        let request = match Request::parse_bytes(frame, u8::MAX) {
            Ok(r) => r,
            Err(_) => {
                counter = counter.wrapping_add(1);
                continue;
            }
        };

        // Alternate the source address family per request to drive both ipv4 and ipv6 maps.
        let src = if counter & 1 == 0 {
            CanonicalSocketAddr::new(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, counter)),
                1024 + counter as u16,
            ))
        } else {
            CanonicalSocketAddr::new(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, counter as u16 + 1)),
                1024 + counter as u16,
            ))
        };
        counter = counter.wrapping_add(1);

        match request {
            Request::Announce(announce_request) => {
                let _response = torrents.announce(
                    &config,
                    &statistics_sender,
                    &mut rng,
                    &announce_request,
                    src,
                    valid_until,
                );
            }
            Request::Scrape(scrape_request) => {
                let _response = torrents.scrape(scrape_request, src);
            }
            // Connect requests carry no swarm state; parsing them already exercised the parser.
            Request::Connect(_) => {}
        }
    }
});
