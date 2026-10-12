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
//! find none (the first ones to a host) look it up each, until one of their lookups fails, which
//! fails the host's requests for [`DNS_RETRY`]. A connection that fails makes its host's lookup
//! due, no sooner than [`DNS_RETRY`] after the last one ended, so an endpoint that moved is looked
//! up again at the next attempt.
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
//! CONNECT proxy's tunnel, and the TLS handshake, which ureq's rustls connector completes) before
//! it writes any byte of the request, it sends a request once and never again on another
//! connection, and it sends no TLS early data.
//! `client::tests::a_failed_tls_handshake_sends_nothing` pins the handshake. These use ureq's
//! `unversioned` resolver and transport types, which its semver promises do not cover: an update
//! of ureq that changes them fails the build here.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
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

/// A host's last successful lookup: its addresses, and when the lookup ended.
#[derive(Clone, Debug)]
struct Good {
    addrs: ResolvedSocketAddrs,
    at: Instant,
}

/// Why a host's last lookup failed, kept to fail the requests that come before the next lookup
/// is due (a ureq error is not `Clone`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    TimedOut,
    NotFound,
    Io(std::io::ErrorKind),
}

impl Failure {
    fn of(e: &ureq::Error) -> Failure {
        match unwrapped(e) {
            ureq::Error::Timeout(_) => Failure::TimedOut,
            ureq::Error::Io(e) => Failure::Io(e.kind()),
            _ => Failure::NotFound,
        }
    }

    fn error(self) -> ureq::Error {
        match self {
            Failure::TimedOut => ureq::Error::Timeout(ureq::Timeout::Resolve),
            Failure::NotFound => ureq::Error::HostNotFound,
            Failure::Io(kind) => ureq::Error::Io(kind.into()),
        }
    }
}

/// What the cache knows of a host.
#[derive(Clone, Debug)]
struct Entry {
    /// The last successful lookup.
    good: Option<Good>,
    /// When the last lookup ended, successful or not.
    ended: Instant,
    /// How the last lookup failed, when it did.
    failed: Option<Failure>,
    /// When the next lookup is due.
    next: Instant,
    /// The lookup a request runs now, by its number.
    looking: Option<u64>,
}

impl Entry {
    /// The last successful lookup's addresses, while they stand in for a lookup at `now`: within
    /// [`DNS_STALE_LIMIT`] of it.
    fn stand_in(&self, now: Instant) -> Option<&Good> {
        self.good
            .as_ref()
            .filter(|good| now.saturating_duration_since(good.at) < DNS_STALE_LIMIT)
    }
}

/// What a request does about its host's addresses.
#[derive(Debug)]
enum Plan {
    /// Uses them: they are fresh, they stand in for a failed lookup until the next is due, or
    /// another request looks the host up.
    Use(ResolvedSocketAddrs),
    /// Fails as the last lookup did, which failed with no addresses to stand in, until the next
    /// lookup is due.
    Fail(Failure),
    /// Looks the host up as lookup `id`, with the addresses that stand in if it fails.
    LookUp(Option<Good>),
}

/// The plan of a request at `now`, given what the cache knows of its host, which the request
/// marks as being looked up (as lookup `id`) when it is to look the host up.
fn plan(entry: &mut Entry, now: Instant, id: u64) -> Plan {
    let stand_in = entry.stand_in(now).cloned();
    match (stand_in, entry.failed) {
        (Some(good), _) if now < entry.next || entry.looking.is_some() => Plan::Use(good.addrs),
        (None, Some(failure)) if now < entry.next => Plan::Fail(failure),
        (stand_in, _) => {
            entry.looking = Some(id);
            Plan::LookUp(stand_in)
        }
    }
}

/// When the next lookup is due after one that failed at `now` with `good` standing in: after
/// [`DNS_RETRY`], and no later than the stale limit of `good`.
fn retry_at(good: &Good, now: Instant) -> Instant {
    let retry = now.checked_add(DNS_RETRY).unwrap_or(now);
    let stale = good.at.checked_add(DNS_STALE_LIMIT).unwrap_or(good.at);
    retry.min(stale)
}

/// When the next lookup is due after a connection to the host's addresses failed at `now`: at
/// once, but no sooner than [`DNS_RETRY`] after the last lookup ended, and no later than it was.
fn due_at(entry: &Entry, now: Instant) -> Instant {
    let floor = entry.ended.checked_add(DNS_RETRY).unwrap_or(entry.ended);
    entry.next.min(now.max(floor))
}

/// The agents' resolver: ureq's own, behind a cache of each host's addresses ([`DNS_FRESH`]),
/// whose last addresses also stand in for failed lookups ([`DNS_STALE_LIMIT`], [`DNS_RETRY`]), one
/// lookup at a time per host while they do (see the module). A lookup that failed with none to
/// stand in fails the host's requests until the next is due, after [`DNS_RETRY`]. An IP address
/// needs no lookup and is never cached. Every failure it returns is [`NotSent`]. Clones share one
/// cache.
#[derive(Debug)]
pub(crate) struct Resolver<R = DefaultResolver>(Arc<Cache<R>>);

#[derive(Debug)]
struct Cache<R> {
    inner: R,
    /// What the cache knows of each host, by host and port.
    hosts: Mutex<HashMap<String, Entry>>,
    /// Numbers the lookups.
    lookups: AtomicU64,
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
            lookups: AtomicU64::new(0),
        }))
    }

    fn hosts(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.0.hosts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Moves every host's lookups `by` into the past, for tests of what follows a lookup's retry.
    #[cfg(test)]
    pub(crate) fn backdate(&self, by: Duration) {
        let back = |at: Instant| at.checked_sub(by).unwrap_or(at);
        for entry in self.hosts().values_mut() {
            entry.ended = back(entry.ended);
            entry.next = back(entry.next);
            if let Some(good) = entry.good.as_mut() {
                good.at = back(good.at);
            }
        }
    }

    /// Makes the lookup of `uri`'s host due (`due_at`) and keeps its addresses: a connection to
    /// them failed, so the endpoint may have moved.
    fn due(&self, uri: &Uri) {
        let Some(host) = host(uri) else {
            return;
        };
        if let Some(entry) = self.hosts().get_mut(&host) {
            entry.next = due_at(entry, Instant::now());
        }
    }
}

/// A request's lookup of `host`, as lookup `id`: ends the mark in the cache however the lookup
/// ends, unless another request looks the host up since.
struct Looking<'a, R> {
    resolver: &'a Resolver<R>,
    host: &'a str,
    id: u64,
}

impl<R> Drop for Looking<'_, R> {
    fn drop(&mut self) {
        if let Some(entry) = self.resolver.hosts().get_mut(self.host)
            && entry.looking == Some(self.id)
        {
            entry.looking = None;
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
        let id = self.0.lookups.fetch_add(1, Ordering::Relaxed);
        let stand_in = {
            let mut hosts = self.hosts();
            let entry = hosts.entry(host.clone()).or_insert(Entry {
                good: None,
                ended: now,
                failed: None,
                next: now,
                looking: None,
            });
            match plan(entry, now, id) {
                Plan::Use(addrs) => return Ok(addrs),
                Plan::Fail(failure) => return Err(not_sent(failure.error())),
                Plan::LookUp(stand_in) => stand_in,
            }
        };
        let _looking = Looking {
            resolver: self,
            host: &host,
            id,
        };
        let looked = self.0.inner.resolve(uri, config, timeout);
        let ended = Instant::now();
        let mut hosts = self.hosts();
        let Some(entry) = hosts.get_mut(&host) else {
            return looked.map_err(not_sent);
        };
        entry.ended = ended;
        match looked {
            Ok(addrs) => {
                entry.good = Some(Good {
                    addrs: addrs.clone(),
                    at: ended,
                });
                entry.failed = None;
                entry.next = ended.checked_add(DNS_FRESH).unwrap_or(ended);
                Ok(addrs)
            }
            Err(e) => {
                entry.failed = Some(Failure::of(&e));
                match stand_in {
                    Some(good) => {
                        entry.next = retry_at(&good, ended);
                        drop(hosts);
                        log::emit(Level::Warn, "dns_stale", &[
                            ("host", &host),
                            (
                                "age_ms",
                                &ended.saturating_duration_since(good.at).as_millis(),
                            ),
                            ("reason", &e),
                        ]);
                        Ok(good.addrs)
                    }
                    None => {
                        entry.next = ended.checked_add(DNS_RETRY).unwrap_or(ended);
                        Err(not_sent(e))
                    }
                }
            }
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
    use std::sync::mpsc;
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
    use super::Entry;
    use super::Resolver;
    use super::is_not_sent;
    use super::not_sent;
    use super::sent_nothing;
    use super::unwrapped;

    const URL: &str = "http://gw.test/b/s";
    const HOST: &str = "gw.test:80";

    /// How long a test waits for a lookup it expects to start: it fails, rather than hangs, when
    /// the lookup does not.
    const STARTS: Duration = Duration::from_secs(10);

    /// One lookup's answer: addresses, a failure, or addresses once the test lets the lookup end
    /// (`gated`).
    #[derive(Debug)]
    enum Answer {
        Addr(SocketAddr),
        Fail(ureq::Error),
        Gated {
            addr: SocketAddr,
            started: mpsc::Sender<()>,
            end: mpsc::Receiver<()>,
        },
    }

    /// An answer of `addr` whose lookup says on the first receiver that it started, and ends when
    /// the sender sends.
    fn gated(addr: SocketAddr) -> (Answer, mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (started, starts) = mpsc::channel();
        let (ends, end) = mpsc::channel();
        (Answer::Gated { addr, started, end }, starts, ends)
    }

    /// A resolver whose lookups answer from its answers in order, and which counts them.
    #[derive(Debug)]
    struct Scripted {
        lookups: AtomicUsize,
        answers: Mutex<Vec<Answer>>,
    }

    impl Scripted {
        fn new(answers: Vec<Answer>) -> Self {
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
                .remove(0);
            let addr = match answer {
                Answer::Addr(addr) => addr,
                Answer::Fail(e) => return Err(e),
                Answer::Gated { addr, started, end } => {
                    started
                        .send(())
                        .map_err(|_closed| ureq::Error::ConnectionFailed)?;
                    end.recv()
                        .map_err(|_closed| ureq::Error::ConnectionFailed)?;
                    addr
                }
            };
            let mut addrs = self.empty();
            addrs.push(addr);
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

    /// A request resolving [`URL`] in a thread of its own.
    fn in_thread(
        resolver: &Resolver<Scripted>,
    ) -> std::thread::JoinHandle<Result<SocketAddr, ureq::Error>> {
        let resolver = resolver.clone();
        std::thread::spawn(move || resolve(&resolver, URL))
    }

    fn entry(resolver: &Resolver<Scripted>) -> Entry {
        resolver.hosts()[HOST].clone()
    }

    /// Moves the host's last lookup `ago` into the past, and its next one due now.
    fn age(resolver: &Resolver<Scripted>, ago: Duration) {
        let mut hosts = resolver.hosts();
        let entry = hosts.get_mut(HOST).unwrap();
        let at = Instant::now().checked_sub(ago).unwrap();
        entry.good.as_mut().unwrap().at = at;
        entry.ended = at;
        entry.next = Instant::now();
    }

    /// Makes the host's next lookup due now, and nothing else.
    fn due_now(resolver: &Resolver<Scripted>) {
        resolver.hosts().get_mut(HOST).unwrap().next = Instant::now();
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    // Requests to a host within 30 s of its lookup make no other.
    #[test]
    fn requests_within_30_s_of_a_lookup_make_none() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Addr(addr(81)),
        ]));
        for _ in 0..3 {
            assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        }
        assert_eq!(lookups(&resolver), 1);
        age(&resolver, DNS_FRESH);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
    }

    // While one request looks a host up, the host's other requests use its last addresses instead
    // of looking it up too.
    #[test]
    fn one_request_at_a_time_looks_a_host_up() {
        let (lookup, started, end) = gated(addr(81));
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            lookup,
            Answer::Addr(addr(99)),
        ]));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(&resolver, DNS_FRESH);
        let looking = in_thread(&resolver);
        started.recv_timeout(STARTS).unwrap();
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 2);
        end.send(()).unwrap();
        assert_eq!(looking.join().unwrap().unwrap(), addr(81));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
    }

    // Requests that find no addresses look the host up each. When one of their lookups ends while
    // another runs, the host stays marked as looked up by the other, so a request when the next
    // lookup is due uses the addresses found instead of looking it up a third time.
    #[test]
    fn a_lookup_that_ends_leaves_the_host_marked_while_another_runs() {
        let (first, first_started, first_end) = gated(addr(81));
        let (second, second_started, second_end) = gated(addr(82));
        let resolver = Resolver::new(Scripted::new(vec![first, second, Answer::Addr(addr(99))]));
        let first_request = in_thread(&resolver);
        first_started.recv_timeout(STARTS).unwrap();
        let second_request = in_thread(&resolver);
        second_started.recv_timeout(STARTS).unwrap();
        first_end.send(()).unwrap();
        assert_eq!(first_request.join().unwrap().unwrap(), addr(81));
        due_now(&resolver);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
        second_end.send(()).unwrap();
        assert_eq!(second_request.join().unwrap().unwrap(), addr(82));
        due_now(&resolver);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(99));
        assert_eq!(lookups(&resolver), 3);
    }

    // A lookup that fails while the host's last addresses stand in serves them, with no other
    // lookup until the retry is due, and then the host is looked up again.
    #[test]
    fn a_failed_lookup_uses_the_last_addresses_until_the_retry() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Fail(ureq::Error::Timeout(ureq::Timeout::Resolve)),
            Answer::Addr(addr(81)),
        ]));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(&resolver, DNS_FRESH);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 2);
        let next = entry(&resolver).next;
        assert!(next > Instant::now() && next <= Instant::now().checked_add(DNS_RETRY).unwrap());
        due_now(&resolver);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 3);
    }

    // Near the stale limit, a failed lookup holds the next one off no longer than the limit.
    #[test]
    fn a_failed_lookup_near_the_stale_limit_holds_the_next_off_no_longer() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Fail(ureq::Error::Timeout(ureq::Timeout::Resolve)),
        ]));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(
            &resolver,
            DNS_STALE_LIMIT.saturating_sub(Duration::from_secs(1)),
        );
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        let entry = entry(&resolver);
        let limit = entry
            .good
            .as_ref()
            .unwrap()
            .at
            .checked_add(DNS_STALE_LIMIT)
            .unwrap();
        assert!(entry.next <= limit, "{:?} past {:?}", entry.next, limit);
    }

    // Past the stale limit, a failed lookup fails as never sent.
    #[test]
    fn a_failed_lookup_past_the_stale_limit_fails_as_never_sent() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Fail(ureq::Error::HostNotFound),
        ]));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(&resolver, DNS_STALE_LIMIT);
        let e = resolve(&resolver, URL).unwrap_err();
        assert!(matches!(sent_nothing(e), (ureq::Error::HostNotFound, true)));
    }

    // A lookup that fails with no addresses to stand in fails the host's requests as it did until
    // the next lookup is due, with no lookup of theirs.
    #[test]
    fn a_failed_lookup_with_no_addresses_fails_the_host_until_the_retry() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Fail(ureq::Error::Timeout(ureq::Timeout::Resolve)),
            Answer::Fail(ureq::Error::HostNotFound),
            Answer::Addr(addr(80)),
        ]));
        for _ in 0..2 {
            let e = resolve(&resolver, URL).unwrap_err();
            assert!(is_not_sent(&e), "{e}");
            assert!(matches!(
                sent_nothing(e),
                (ureq::Error::Timeout(ureq::Timeout::Resolve), true)
            ));
        }
        assert_eq!(lookups(&resolver), 1);
        due_now(&resolver);
        let e = resolve(&resolver, URL).unwrap_err();
        assert!(matches!(sent_nothing(e), (ureq::Error::HostNotFound, true)));
        due_now(&resolver);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 3);
    }

    // A failed connection makes the host's lookup due, but no sooner than 5 s after the last
    // lookup, and keeps its addresses to stand in for a lookup that fails.
    #[test]
    fn a_failed_connection_makes_the_lookup_due_after_the_retry_and_keeps_the_addresses() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Fail(ureq::Error::Timeout(ureq::Timeout::Resolve)),
        ]));
        let uri: Uri = URL.parse().unwrap();
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        resolver.due(&uri);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 1);
        resolver.hosts().get_mut(HOST).unwrap().ended =
            Instant::now().checked_sub(DNS_RETRY).unwrap();
        resolver.due(&uri);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 2);
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

    // An IP address is never cached, and each scheme's port of a host is its own entry: `https://`
    // without a port is port 443, not 80.
    #[test]
    fn ip_addresses_pass_through_and_ports_are_separate_hosts() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(1)),
            Answer::Addr(addr(2)),
            Answer::Addr(addr(3)),
            Answer::Addr(addr(4)),
            Answer::Addr(addr(5)),
        ]));
        assert_eq!(
            resolve(&resolver, "http://127.0.0.1:9/b/s").unwrap(),
            addr(1)
        );
        assert_eq!(resolve(&resolver, "http://[::1]:9/b/s").unwrap(), addr(2));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(3));
        assert_eq!(resolve(&resolver, "https://gw.test/b/s").unwrap(), addr(4));
        assert_eq!(
            resolve(&resolver, "http://gw.test:8080/b/s").unwrap(),
            addr(5)
        );
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(3));
        assert_eq!(
            resolve(&resolver, "https://gw.test:443/b/s").unwrap(),
            addr(4)
        );
        let mut hosts = resolver.hosts().keys().cloned().collect::<Vec<_>>();
        hosts.sort();
        assert_eq!(hosts, ["gw.test:443", "gw.test:80", "gw.test:8080"]);
    }
}
