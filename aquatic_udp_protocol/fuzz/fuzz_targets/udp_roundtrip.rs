#![no_main]
use aquatic_udp_protocol::Request;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|request: Request| {
    let mut request = request;
    let mut buf = Vec::new();

    request
        .clone()
        .write(&mut buf)
        .expect("Vec write doesn't fail");

    let mut r2 = Request::from_bytes(&buf, u8::MAX).expect("serialized bytes should deserialize");
    normalize_ip(&mut request);
    normalize_ip(&mut r2);
    assert_eq!(request, r2, "roundtrip failure");
});

fn normalize_ip(r: &mut Request) {
    if let Request::Announce(ref mut a) = r {
        if a.ip_address.is_none() {
            a.ip_address = Some([0, 0, 0, 0].into());
        }
    }
}
