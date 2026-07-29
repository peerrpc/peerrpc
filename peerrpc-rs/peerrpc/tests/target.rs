use peerrpc::{parse_target, format_target, Scheme, RoleHint, Target, TargetParseError};

#[test]
fn parse_connect_with_port_and_query() {
    let t = parse_target(
        "peerrpc+connect://signal.example.com:443/echo.Echo?as=client&peer=alice&token=jwt",
    )
    .unwrap();
    assert_eq!(t, Target {
        scheme: Scheme::Connect,
        signal: "signal.example.com:443".into(),
        service: "echo.Echo".into(),
        role: Some(RoleHint::Client),
        peer_id: Some("alice".into()),
        token: Some("jwt".into()),
    });
}

#[test]
fn parse_local_empty_authority() {
    let t = parse_target("peerrpc+local:///echo.Echo").unwrap();
    assert_eq!(t, Target {
        scheme: Scheme::Local,
        signal: String::new(),
        service: "echo.Echo".into(),
        role: None,
        peer_id: None,
        token: None,
    });
}

#[test]
fn parse_ws() {
    let t = parse_target("peerrpc+ws://signal.example.com/echo.Echo").unwrap();
    assert_eq!(t.scheme, Scheme::Ws);
    assert_eq!(t.signal, "signal.example.com");
    assert_eq!(t.service, "echo.Echo");
}

#[test]
fn parse_bare_host_no_port() {
    let t = parse_target("peerrpc+connect://signal.example.com/echo.Echo").unwrap();
    assert_eq!(t.signal, "signal.example.com");
    assert_eq!(t.service, "echo.Echo");
}

#[test]
fn reject_missing_prefix() {
    assert!(matches!(
        parse_target("connect://x/y"),
        Err(TargetParseError::MissingPrefix { .. })
    ));
}

#[test]
fn reject_missing_service() {
    assert!(matches!(
        parse_target("peerrpc+connect://signal.example.com"),
        Err(TargetParseError::MissingService(_))
    ));
    assert!(matches!(
        parse_target("peerrpc+connect://signal.example.com/"),
        Err(TargetParseError::MissingService(_))
    ));
}

#[test]
fn reject_non_local_without_authority() {
    assert!(matches!(
        parse_target("peerrpc+connect:///echo.Echo"),
        Err(TargetParseError::EmptyAuthority { .. })
    ));
}

#[test]
fn reject_unknown_scheme() {
    assert!(matches!(
        parse_target("peerrpc+bogus://x/y"),
        Err(TargetParseError::UnknownScheme(_))
    ));
}

#[test]
fn round_trip_via_format() {
    let original = Target {
        scheme: Scheme::Connect,
        signal: "signal.example.com:443".into(),
        service: "echo.Echo".into(),
        role: Some(RoleHint::Client),
        peer_id: Some("alice".into()),
        token: Some("tok".into()),
    };
    let s = format_target(&original);
    let parsed = parse_target(&s).unwrap();
    assert_eq!(parsed, original);
}
