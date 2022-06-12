#![no_main]
use libfuzzer_sys::fuzz_target;

use aquatic_common::{CanonicalSocketAddr, ValidUntil};
use aquatic_udp::common::*;
use aquatic_udp::config::Config;
use aquatic_udp::workers::swarm::TorrentMaps;
use aquatic_udp::workers::swarm::{handle_announce_request, handle_scrape_request};

use rand::{rngs::SmallRng, SeedableRng};

use std::net::IpAddr;

fuzz_target!(|data: (Vec<(ConnectedRequest, CanonicalSocketAddr)>, u8)| {
    let config = Config::default();

    /* Based on aquatic_udp::workers::request::run_request_worker */

    let mut torrents = TorrentMaps::default();
    let mut rng = SmallRng::from_seed([data.1; 32]);

    let peer_valid_until = ValidUntil::new(config.cleaning.max_peer_age);

    for (request, src) in data.0 {
        let _response = match (request, src.get().ip()) {
            (ConnectedRequest::Announce(request), IpAddr::V4(ip)) => {
                let response = handle_announce_request(
                    &config,
                    &mut rng,
                    &mut torrents.ipv4,
                    request,
                    ip,
                    peer_valid_until,
                );

                ConnectedResponse::AnnounceIpv4(response)
            }
            (ConnectedRequest::Announce(request), IpAddr::V6(ip)) => {
                let response = handle_announce_request(
                    &config,
                    &mut rng,
                    &mut torrents.ipv6,
                    request,
                    ip,
                    peer_valid_until,
                );

                ConnectedResponse::AnnounceIpv6(response)
            }
            (ConnectedRequest::Scrape(request), IpAddr::V4(_)) => {
                ConnectedResponse::Scrape(handle_scrape_request(&mut torrents.ipv4, request))
            }
            (ConnectedRequest::Scrape(request), IpAddr::V6(_)) => {
                ConnectedResponse::Scrape(handle_scrape_request(&mut torrents.ipv6, request))
            }
        };
    }
});
