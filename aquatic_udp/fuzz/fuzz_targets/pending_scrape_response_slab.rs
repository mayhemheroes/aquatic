#![no_main]
use libfuzzer_sys::fuzz_target;

use aquatic_udp::workers::socket::test_pending_scrape_response_slab;

fuzz_target!(|data: (Vec<(i32, i64, u8)>, u8)| {
    assert!(!test_pending_scrape_response_slab(data.0, data.1).is_failure());
});
