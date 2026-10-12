//! The transport under the client's HTTP agents: a cache of the hosts' addresses, and the failures
//! that prove a request never went out.
//!
//! ureq resolves a request's host before it looks for a pooled connection (ureq 3.4.2,
//! `run::connect`), so without the cache every request, on a pooled connection too, asked the
//! system's resolver. In Kubernetes (`ndots:5`, a short name) that is several DNS queries per
//! request: 128 owners sent CoreDNS about 1,300 queries a second, and a lookup that stalled failed
//! its request after `CONNECT_TIMEOUT` (`client::new_agent`) with a pooled connection ready. With
//! the cache, a host is looked up at most once every [`DNS_FRESH`] (every [`DNS_RETRY`] while
//! lookups fail), by one request at a time: while it waits for the lookup, up to the resolve
//! timeout (5 s), the host's other requests go on with its last addresses, and only requests that
//! find none (the first ones to a host) look it up each. A connection that fails makes its host's
//! lookup due, so an endpoint that moved is looked up again at the next attempt.
//!
//! The resolver and the connector here wrap each failure of theirs in [`NotSent`]: a host that did
//! not resolve, and a connection that was not made. Nothing of such a request went out, so an
//! append whose every attempt failed this way applied nothing (`client::append`). A failure on a
//! connection that was made, a pooled one included, is never wrapped: part of the request, or all
//! of it, may have gone out. The wrapping is by where the failure happened, not by its kind: the
//! same I/O error kinds (a reset, `InvalidData` from rustls, an unreachable host) also come from
//! connections that carried a request.
//!
//! That proof relies on how ureq 3.4.2 sends a request as these agents use it (no redirects
//! followed, no middleware): it resolves the host and makes the connection (the TCP connect, a
//! CONNECT proxy's tunnel, and the TLS handshake, which ureq's rustls connector completes) before it
//! writes any byte of the request, it sends a request once and never again on another connection,
//! and it sends no TLS early data. `client::tests::a_failed_tls_handshake_sends_nothing` pins the
//! handshake. These use ureq's `unversioned` resolver and transport types, which its semver promises
//! do not cover: an update of ureq that changes them fails the build here.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use ureq::config::Config;
use ureq::http::Uri;
use ureq::unversioned::resolver::DefaultResolver;
use ureq::unversioned::resolver::ResolvedSocketAddrs;
use ureq::unversioned::transport::ConnectionDetails;
use ureq::unversioned::transport::DefaultConnector;
use ureq::unversioned::transport::NextTimeout;
use ureq::unversioned::transport::Transport;

use crate::log;
use crate::log::Level;

/// How long a host's addresses serve requests after a lookup, without another one.
pub(crate) const DNS_FRESH: Duration = Duration::from_secs(30);

/// How long after a host's last successful lookup its addresses still serve requests while new
/// lookups fail. Past it, a request whose lookup fails fails with it.
pub(crate) const DNS_STALE_LIMIT: Duration = Duration::from_secs(600);

/// How long after a failed lookup the host's last addresses serve requests without another
/// lookup.
pub(crate) const DNS_RETRY: Duration = Duration::from_secs(5);

/// A failure of a request before any byte of it went out (see the module): its host did not
/// resolve, or no connection to it was made. It shows as the failure it wraps.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(crate) struct NotSent(ureq::Error);

/// `e` wrapped in [`NotSent`], once: a CONNECT proxy's tunnel is made through the same resolver
/// and connector, whose failure the connector would otherwise wrap again.
fn not_sent(e: ureq::Error) -> ureq::Error {
    if is_not_sent(&e) {
        return e;
    }
    ureq::Error::Other(Box::new(NotSent(e)))
}

/// The failure as ureq reported it, without the [`NotSent`] wrapping, and whether it proves that
/// nothing of the request went out.
pub(crate) fn sent_nothing(e: ureq::Error) -> (ureq::Error, bool) {
    match e {
        ureq::Error::Other(other) => match other.downcast::<NotSent>() {
            Ok(not_sent) => (not_sent.0, true),
            Err(other) => (ureq::Error::Other(other), false),
        },
        e => (e, false),
    }
}

/// The failure as ureq reported it, without the [`NotSent`] wrapping.
pub(crate) fn unwrapped(e: &ureq::Error) -> &ureq::Error {
    match e {
        ureq::Error::Other(other) => other
            .downcast_ref::<NotSent>()
            .map_or(e, |not_sent| &not_sent.0),
        e => e,
    }
}

/// Whether `e` proves that nothing of the request went out (see the module).
pub(crate) fn is_not_sent(e: &ureq::Error) -> bool {
    matches!(e, ureq::Error::Other(other) if other.is::<NotSent>())
}

/// A host's addresses from its last successful lookup, when that was, when the next lookup is
/// due, and whether a request looks it up now.
#[derive(Clone, Debug)]
struct Looked {
    addrs: ResolvedSocketAddrs,
    at: Instant,
    next: Instant,
    looking: bool,
}

/// What a request does about its host's addresses.
#[derive(Debug)]
enum Plan {
    /// Uses them: they are fresh, or another request looks the host up and they still stand in.
    Use(ResolvedSocketAddrs),
    /// Looks the host up, with the last addresses to stand in if the lookup fails.
    LookUp(Option<Looked>),
}

/// The plan of a request at `now`, given its host's last lookup, which it marks as being looked
/// up when the request is to look it up.
fn plan(last: Option<&mut Looked>, now: Instant) -> Plan {
    match last {
        Some(last) if now < last.next || (last.looking && stands_in(last, now)) => {
            Plan::Use(last.addrs.clone())
        }
        Some(last) => {
            last.looking = true;
            Plan::LookUp(Some(last.clone()))
        }
        None => Plan::LookUp(None),
    }
}

/// Whether the host's last addresses stand in for a lookup at `now`: within [`DNS_STALE_LIMIT`]
/// of the last successful lookup.
fn stands_in(last: &Looked, now: Instant) -> bool {
    now.saturating_duration_since(last.at) < DNS_STALE_LIMIT
}

/// When the next lookup is due after one failed at `now` and `last` stood in: after
/// [`DNS_RETRY`], and no later than its stale limit.
fn retry_at(last: &Looked, now: Instant) -> Instant {
    let retry = now.checked_add(DNS_RETRY).unwrap_or(now);
    let stale = last.at.checked_add(DNS_STALE_LIMIT).unwrap_or(last.at);
    retry.min(stale)
}

/// The agents' resolver: ureq's own, behind a cache of each host's addresses ([`DNS_FRESH`]),
/// whose last addresses also stand in for failed lookups ([`DNS_STALE_LIMIT`], [`DNS_RETRY`]), one
/// lookup at a time per host (see the module). An IP address needs no lookup and is never cached.
/// Every failure it returns is [`NotSent`]. Clones share one cache.
#[derive(Debug)]
pub(crate) struct Resolver<R = DefaultResolver>(Arc<Cache<R>>);

#[derive(Debug)]
struct Cache<R> {
    inner: R,
    /// Each host's last successful lookup, by host and port.
    hosts: Mutex<HashMap<String, Looked>>,
}

impl<R> Clone for Resolver<R> {
    fn clone(&self) -> Self {
        Resolver(Arc::clone(&self.0))
    }
}

impl Resolver {
    /// The resolver both agents share, so that a host looked up for one serves the other.
    pub(crate) fn shared() -> Resolver {
        static R: OnceLock<Resolver> = OnceLock::new();
        R.get_or_init(|| Resolver::new(DefaultResolver::default()))
            .clone()
    }
}

impl<R> Resolver<R> {
    pub(crate) fn new(inner: R) -> Self {
        Resolver(Arc::new(Cache {
            inner,
            hosts: Mutex::new(HashMap::new()),
        }))
    }

    fn hosts(&self) -> std::sync::MutexGuard<'_, HashMap<String, Looked>> {
        self.0.hosts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes the lookup of `uri`'s host due and keeps its addresses: a connection to them failed,
    /// so the endpoint may have moved.
    fn due(&self, uri: &Uri) {
        let Some(host) = host(uri) else {
            return;
        };
        if let Some(last) = self.hosts().get_mut(&host) {
            last.next = Instant::now();
        }
    }
}

/// A request's lookup of `host`: ends it in the cache, however the lookup ends, so that another
/// request may look the host up.
struct Looking<'a, R> {
    resolver: &'a Resolver<R>,
    host: &'a str,
}

impl<R> Drop for Looking<'_, R> {
    fn drop(&mut self) {
        if let Some(last) = self.resolver.hosts().get_mut(self.host) {
            last.looking = false;
        }
    }
}

/// The host and port `uri` connects to, unless its host is an IP address, which needs no lookup.
fn host(uri: &Uri) -> Option<String> {
    let scheme = uri.scheme()?;
    let authority = uri.authority()?;
    if authority
        .host()
        .trim_matches(['[', ']'])
        .parse::<IpAddr>()
        .is_ok()
    {
        return None;
    }
    DefaultResolver::host_and_port(scheme, authority)
}

impl<R: ureq::unversioned::resolver::Resolver> ureq::unversioned::resolver::Resolver
    for Resolver<R>
{
    fn resolve(
        &self,
        uri: &Uri,
        config: &Config,
        timeout: NextTimeout,
    ) -> Result<ResolvedSocketAddrs, ureq::Error> {
        let Some(host) = host(uri) else {
            return self.0.inner.resolve(uri, config, timeout).map_err(not_sent);
        };
        let now = Instant::now();
        let last = match plan(self.hosts().get_mut(&host), now) {
            Plan::Use(addrs) => return Ok(addrs),
            Plan::LookUp(last) => last,
        };
        let _looking = Looking {
            resolver: self,
            host: &host,
        };
        match self.0.inner.resolve(uri, config, timeout) {
            Ok(addrs) => {
                let at = Instant::now();
                self.hosts().insert(host.clone(), Looked {
                    addrs: addrs.clone(),
                    at,
                    next: at.checked_add(DNS_FRESH).unwrap_or(at),
                    looking: false,
                });
                Ok(addrs)
            }
            Err(e) => match last.filter(|last| stands_in(last, now)) {
                Some(last) => {
                    log::emit(Level::Warn, "dns_stale", &[
                        ("host", &host),
                        (
                            "age_ms",
                            &now.saturating_duration_since(last.at).as_millis(),
                        ),
                        ("reason", &e),
                    ]);
                    if let Some(entry) = self.hosts().get_mut(&host) {
                        entry.next = retry_at(&last, Instant::now());
                    }
                    Ok(last.addrs)
                }
                None => Err(not_sent(e)),
            },
        }
    }
}

/// The agents' connector: ureq's own (a CONNECT proxy's tunnel, TCP, then TLS for `https://`),
/// whose every failure is [`NotSent`]: no connection was made, so nothing of the request went
/// out. A failure also makes the lookup of the request's host due ([`Resolver`]).
#[derive(Debug)]
pub(crate) struct Connector<R = DefaultResolver> {
    inner: DefaultConnector,
    resolver: Resolver<R>,
}

impl<R> Connector<R> {
    /// A connector that makes `resolver`'s lookups due when a connection fails.
    pub(crate) fn new(resolver: Resolver<R>) -> Self {
        Connector {
            inner: DefaultConnector::default(),
            resolver,
        }
    }
}

impl<R: std::fmt::Debug + Send + Sync + 'static> ureq::unversioned::transport::Connector
    for Connector<R>
{
    type Out = Box<dyn Transport>;

    fn connect(
        &self,
        details: &ConnectionDetails,
        chained: Option<()>,
    ) -> Result<Option<Self::Out>, ureq::Error> {
        let e = match self.inner.connect(details, chained) {
            Ok(Some(transport)) => return Ok(Some(transport)),
            Ok(None) => ureq::Error::ConnectionFailed,
            Err(e) => e,
        };
        self.resolver.due(details.uri);
        Err(not_sent(e))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use std::sync::PoisonError;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::time::Instant;

    use ureq::config::Config;
    use ureq::http::Uri;
    use ureq::unversioned::resolver::ResolvedSocketAddrs;
    use ureq::unversioned::resolver::Resolver as _;
    use ureq::unversioned::transport::NextTimeout;

    use super::DNS_FRESH;
    use super::DNS_RETRY;
    use super::DNS_STALE_LIMIT;
    use super::Looked;
    use super::Plan;
    use super::Resolver;
    use super::is_not_sent;
    use super::not_sent;
    use super::plan;
    use super::retry_at;
    use super::sent_nothing;
    use super::stands_in;
    use super::unwrapped;

    /// A resolver whose lookups answer from `answers`, one per lookup, and which counts them.
    #[derive(Debug)]
    struct Scripted {
        lookups: AtomicUsize,
        answers: Mutex<Vec<Result<SocketAddr, ureq::Error>>>,
    }

    impl Scripted {
        fn new(answers: Vec<Result<SocketAddr, ureq::Error>>) -> Self {
            Scripted {
                lookups: AtomicUsize::new(0),
                answers: Mutex::new(answers),
            }
        }
    }

    impl ureq::unversioned::resolver::Resolver for Scripted {
        fn resolve(
            &self,
            _uri: &Uri,
            _config: &Config,
            _timeout: NextTimeout,
        ) -> Result<ResolvedSocketAddrs, ureq::Error> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            let answer = self
                .answers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(0)?;
            let mut addrs = self.empty();
            addrs.push(answer);
            Ok(addrs)
        }
    }

    fn lookups(resolver: &Resolver<Scripted>) -> usize {
        resolver.0.inner.lookups.load(Ordering::Relaxed)
    }

    fn resolve(resolver: &Resolver<Scripted>, uri: &str) -> Result<SocketAddr, ureq::Error> {
        let uri: Uri = uri
            .parse()
            .map_err(|e: ureq::http::uri::InvalidUri| ureq::Error::BadUri(e.to_string()))?;
        let timeout = NextTimeout {
            after: Duration::from_secs(5).into(),
            reason: ureq::Timeout::Resolve,
        };
        resolver
            .resolve(&uri, &Config::default(), timeout)
            .map(|addrs| addrs[0])
    }

    /// Moves `host`'s last lookup `ago` into the past, and its next one due now.
    fn age(resolver: &Resolver<Scripted>, host: &str, ago: Duration) {
        let mut hosts = resolver.hosts();
        let last = hosts.get_mut(host).unwrap();
        last.at = Instant::now().checked_sub(ago).unwrap();
        last.next = Instant::now();
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    #[test]
    fn a_lookup_serves_until_due_and_stands_in_for_failed_ones_for_10_min() {
        let now = Instant::now();
        let looked = |ago: Duration, next_in: Duration| Looked {
            addrs: ResolvedSocketAddrs::from_fn(|_| addr(0)),
            at: now.checked_sub(ago).unwrap(),
            next: now.checked_add(next_in).unwrap(),
            looking: false,
        };
        assert!(matches!(plan(None, now), Plan::LookUp(None)));
        assert!(matches!(
            plan(Some(&mut looked(Duration::ZERO, DNS_FRESH)), now),
            Plan::Use(_)
        ));
        let mut due = looked(DNS_FRESH, Duration::ZERO);
        assert!(matches!(plan(Some(&mut due), now), Plan::LookUp(Some(_))));
        assert!(due.looking);
        let almost = DNS_STALE_LIMIT.saturating_sub(Duration::from_millis(1));
        assert!(stands_in(&looked(almost, Duration::ZERO), now));
        assert!(!stands_in(&looked(DNS_STALE_LIMIT, Duration::ZERO), now));
    }

    // While one request looks a host up, the others use its last addresses, which still stand in,
    // instead of waiting for lookups of their own. Past the stale limit, they look it up too.
    #[test]
    fn one_request_at_a_time_looks_a_host_up() {
        let now = Instant::now();
        let mut looking = Looked {
            addrs: ResolvedSocketAddrs::from_fn(|_| addr(0)),
            at: now.checked_sub(DNS_FRESH).unwrap(),
            next: now,
            looking: true,
        };
        assert!(matches!(plan(Some(&mut looking), now), Plan::Use(_)));
        looking.at = now.checked_sub(DNS_STALE_LIMIT).unwrap();
        assert!(matches!(
            plan(Some(&mut looking), now),
            Plan::LookUp(Some(_))
        ));
    }

    // A failed lookup holds the next one off for 5 s, but not past the last addresses' stale
    // limit.
    #[test]
    fn a_failed_lookup_holds_the_next_off_until_the_stale_limit_at_most() {
        let now = Instant::now();
        let looked = |ago: Duration| Looked {
            addrs: ResolvedSocketAddrs::from_fn(|_| addr(0)),
            at: now.checked_sub(ago).unwrap(),
            next: now,
            looking: true,
        };
        let early = looked(DNS_FRESH);
        assert_eq!(retry_at(&early, now), now.checked_add(DNS_RETRY).unwrap());
        let late = looked(DNS_STALE_LIMIT.saturating_sub(Duration::from_secs(1)));
        assert_eq!(
            retry_at(&late, now),
            late.at.checked_add(DNS_STALE_LIMIT).unwrap()
        );
    }

    // A failure is wrapped once, however many times it passes a marking resolver or connector (a
    // CONNECT proxy's tunnel goes through both, and then the connector's own wrapping).
    #[test]
    fn a_failure_is_marked_once() {
        let twice = not_sent(not_sent(ureq::Error::Timeout(ureq::Timeout::Connect)));
        assert!(matches!(
            unwrapped(&twice),
            ureq::Error::Timeout(ureq::Timeout::Connect)
        ));
        assert!(matches!(
            sent_nothing(twice),
            (ureq::Error::Timeout(ureq::Timeout::Connect), true)
        ));
    }

    // A failed connection makes its host's lookup due, so a moved endpoint is looked up again at
    // the next attempt.
    #[test]
    fn a_failed_connection_makes_the_lookup_due() {
        let resolver = Resolver::new(Scripted::new(vec![Ok(addr(80)), Ok(addr(81))]));
        assert_eq!(resolve(&resolver, "http://gw.test/b/s").unwrap(), addr(80));
        resolver.due(&"http://gw.test/b/s".parse::<Uri>().unwrap());
        assert_eq!(resolve(&resolver, "http://gw.test/b/s").unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
    }

    // Requests to a host within 30 s of its lookup make no other.
    #[test]
    fn requests_within_30_s_of_a_lookup_make_none() {
        let resolver = Resolver::new(Scripted::new(vec![Ok(addr(80)), Ok(addr(81))]));
        for _ in 0..3 {
            assert_eq!(resolve(&resolver, "http://gw.test/b/s").unwrap(), addr(80));
        }
        assert_eq!(lookups(&resolver), 1);
        age(&resolver, "gw.test:80", DNS_FRESH);
        assert_eq!(resolve(&resolver, "http://gw.test/b/s").unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
    }

    // A lookup that times out (`Timeout::Resolve`) or finds no host fails as never sent while the
    // host has no addresses, and later serves its last addresses within 10 min of their lookup,
    // without another lookup for 5 s.
    #[test]
    fn a_failed_lookup_uses_the_last_addresses_for_10_min_then_fails_as_never_sent() {
        let resolver = Resolver::new(Scripted::new(vec![
            Err(ureq::Error::Timeout(ureq::Timeout::Resolve)),
            Ok(addr(80)),
            Err(ureq::Error::Timeout(ureq::Timeout::Resolve)),
            Err(ureq::Error::HostNotFound),
        ]));
        let e = resolve(&resolver, "http://gw.test/b/s").unwrap_err();
        assert!(is_not_sent(&e), "{e}");
        assert!(matches!(
            sent_nothing(e),
            (ureq::Error::Timeout(ureq::Timeout::Resolve), true)
        ));
        assert_eq!(resolve(&resolver, "http://gw.test/b/s").unwrap(), addr(80));
        age(&resolver, "gw.test:80", DNS_FRESH);
        assert_eq!(resolve(&resolver, "http://gw.test/b/s").unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 3);
        // The failed lookup holds the next one off for DNS_RETRY.
        let next = resolver.hosts().get("gw.test:80").unwrap().next;
        assert!(next > Instant::now() && next <= Instant::now().checked_add(DNS_RETRY).unwrap());
        assert_eq!(resolve(&resolver, "http://gw.test/b/s").unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 3);
        age(&resolver, "gw.test:80", DNS_STALE_LIMIT);
        let e = resolve(&resolver, "http://gw.test/b/s").unwrap_err();
        assert!(matches!(sent_nothing(e), (ureq::Error::HostNotFound, true)));
        assert_eq!(lookups(&resolver), 4);
    }

    // An IP address is never cached, and each scheme's port of a host is its own entry: `https://`
    // without a port is port 443, not 80.
    #[test]
    fn ip_addresses_pass_through_and_ports_are_separate_hosts() {
        let resolver = Resolver::new(Scripted::new(vec![
            Ok(addr(1)),
            Ok(addr(2)),
            Ok(addr(3)),
            Ok(addr(4)),
            Ok(addr(5)),
        ]));
        assert_eq!(
            resolve(&resolver, "http://127.0.0.1:9/b/s").unwrap(),
            addr(1)
        );
        assert_eq!(resolve(&resolver, "http://[::1]:9/b/s").unwrap(), addr(2));
        assert_eq!(resolve(&resolver, "http://gw.test/b/s").unwrap(), addr(3));
        assert_eq!(resolve(&resolver, "https://gw.test/b/s").unwrap(), addr(4));
        assert_eq!(
            resolve(&resolver, "http://gw.test:8080/b/s").unwrap(),
            addr(5)
        );
        assert_eq!(resolve(&resolver, "http://gw.test/b/s").unwrap(), addr(3));
        assert_eq!(
            resolve(&resolver, "https://gw.test:443/b/s").unwrap(),
            addr(4)
        );
        let mut hosts = resolver.hosts().keys().cloned().collect::<Vec<_>>();
        hosts.sort();
        assert_eq!(hosts, ["gw.test:443", "gw.test:80", "gw.test:8080"]);
    }
}
