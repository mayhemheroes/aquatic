// Mayhem fuzz target: udp_roundtrip
//
// Honest reconstruction of the old `udp_roundtrip` target. The original (in the pre-refactor
// fork at aquatic_udp_protocol/fuzz/fuzz_targets/udp_roundtrip.rs) took an `arbitrary`-derived
// `Request`, serialized it with `Request::write`, re-parsed it with `Request::from_bytes`, and
// asserted the round-trip is the identity. Current upstream (greatest-ape/aquatic) renamed those
// to `Request::write_bytes` / `Request::parse_bytes`, removed the `arbitrary::Arbitrary` derive on
// the protocol types (it now lives only behind `#[cfg(test)]` as a quickcheck impl), and switched
// to zerocopy parsing.
//
// We drive the SAME real code path — `aquatic_udp_protocol::Request::{parse_bytes, write_bytes}` —
// from raw fuzzer bytes: parse arbitrary bytes into a `Request`, then assert the
// serialize -> parse round-trip is stable (matching the protocol crate's own
// `same_after_conversion` quickcheck oracle in crates/udp_protocol/src/request.rs). This exercises
// the actual UDP BitTorrent request parser and serializer, not a stub.
#![no_main]

use aquatic_udp_protocol::Request;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // First parse: arbitrary fuzzer bytes -> Request. Most inputs are rejected; this still
    // exercises the full parser (action dispatch, zerocopy reads, length checks).
    let request = match Request::parse_bytes(data, u8::MAX) {
        Ok(r) => r,
        Err(_) => return,
    };

    // Serialize the parsed request back to the wire format.
    let mut buf = Vec::new();
    request
        .write_bytes(&mut buf)
        .expect("Vec write doesn't fail");

    // Re-parse the bytes we just wrote: a well-formed Request must serialize to bytes that parse
    // back into an equal Request. A regression in either parser or serializer breaks this.
    let mut r1 = request;
    let mut r2 = Request::parse_bytes(&buf, u8::MAX)
        .expect("bytes produced by write_bytes should parse");

    normalize(&mut r1);
    normalize(&mut r2);

    assert_eq!(r1, r2, "roundtrip failure");
});

// The original harness normalized an absent announce IP to 0.0.0.0 before comparing, because the
// wire format has no distinct "absent" encoding. Current upstream uses a fixed-size
// `Ipv4AddrBytes` field, so a parsed Request is already canonical and this is a no-op kept for
// parity / future-proofing.
fn normalize(_r: &mut Request) {}
