//! Immutable reads at a read origin (extensions §4; design doc `immutable-reads.md` §4): attach
//! fetches the snapshot and the history after it from a second origin, typically a CDN in front of
//! the same server, and the rest from the stream's own origin.
//!
//! The read origin is `URSULA_VFS_READ_ORIGIN` when it names one (`off` turns reads there off),
//! else the `Stream-Read-Origin` hint of attach's `HEAD`, followed only when it is an `https`
//! origin within the stream URL's site (its registrable domain, from the public suffix list).
//! Requests to it carry the token like every other request: a hint outside the site is never
//! followed, and a configured origin is trusted. Only attach's snapshot `GET` and its history reads
//! go there, both with the incarnation in the URL; writes, `HEAD`, the claim, `consistency=leader`
//! reads, the snapshot thread and retention never do.
//!
//! Every answer is checked (`client::read_immutable`, `client::get_snapshot_immutable`). The first
//! that fails a check, or a transport error, ends the read origin for the rest of the attach,
//! logged once (`read_origin_fallback`), and the request is sent again to the stream's origin.
//! Nothing the read origin answers is taken for a recreated stream: only the stream origin's 412
//! says that. The history reads also end, without a log line, at the tail `HEAD` named or at the
//! first answer not marked `Stream-Immutable` (the rest is then within a chunk of the tail).

use std::net::IpAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use ureq::http::Uri;
use ureq::http::uri::Authority;

use crate::client::get_snapshot_immutable;
use crate::client::offset_token;
use crate::client::read_immutable;
use crate::config::ReadOriginSetting;
use crate::config::read_origin;
use crate::error::Error;
use crate::log;
use crate::log::Level;
use crate::snapshot::Snapshot;

/// An `https` origin: a host and an optional port, with no path, query or userinfo. Kept as
/// `https://host[:port]`, lowercased.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Origin {
    origin: String,
    host: String,
}

impl Origin {
    /// `value` as an origin, or `None` unless it is exactly `https://host[:port]`.
    pub(crate) fn parse(value: &str) -> Option<Origin> {
        let scheme = value.get(..8)?;
        let rest = value.get(8..)?;
        if !scheme.eq_ignore_ascii_case("https://")
            || rest.is_empty()
            || rest.contains(['/', '?', '#', '@', '\\'])
            || rest.contains(|c: char| c.is_whitespace() || c.is_control())
        {
            return None;
        }
        let authority = rest.parse::<Authority>().ok()?;
        // Rebuilt from its parts, so that an empty or unusable port (`host:`) is refused too.
        let rebuilt = match authority.port_u16() {
            Some(port) => format!("{}:{port}", authority.host()),
            None => authority.host().to_owned(),
        };
        if authority.host().is_empty() || !rebuilt.eq_ignore_ascii_case(rest) {
            return None;
        }
        Some(Origin {
            origin: format!("https://{}", authority.as_str().to_ascii_lowercase()),
            host: authority.host().to_ascii_lowercase(),
        })
    }

    /// Any origin, `http` included, for tests against a local stand-in.
    #[cfg(test)]
    pub(crate) fn for_tests(origin: &str) -> Origin {
        let uri = origin.parse::<Uri>().unwrap();
        Origin {
            origin: origin.trim_end_matches('/').to_owned(),
            host: uri.host().unwrap().to_owned(),
        }
    }
}

impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.origin)
    }
}

/// Why the read origin hint is not followed.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Ignored {
    #[error("{hint:?} is not an https origin without a path")]
    NotHttpsOrigin { hint: String },
    #[error("{hint} is outside the site of {stream}")]
    OtherSite { hint: String, stream: String },
}

/// The read origin of an attach of stream `url`: the setting's, or `hint` (a `Stream-Read-Origin`)
/// when the setting leaves it to the hint and the hint passes the guards. `None` for none.
pub(crate) fn chosen(
    setting: &ReadOriginSetting,
    hint: Option<&str>,
    url: &str,
) -> Result<Option<Origin>, Ignored> {
    let hint = match setting {
        ReadOriginSetting::Off => return Ok(None),
        ReadOriginSetting::Configured(origin) => return Ok(Some(origin.clone())),
        ReadOriginSetting::Hint => match hint.map(str::trim) {
            None | Some("") => return Ok(None),
            Some(hint) => hint,
        },
    };
    let Some(origin) = Origin::parse(hint) else {
        return Err(Ignored::NotHttpsOrigin {
            hint: hint.to_owned(),
        });
    };
    let stream = url.parse::<Uri>().ok();
    let stream = stream.as_ref().and_then(Uri::host).unwrap_or_default();
    if !same_site(stream, &origin.host) {
        return Err(Ignored::OtherSite {
            hint: origin.origin,
            stream: stream.to_owned(),
        });
    }
    Ok(Some(origin))
}

/// Whether hosts `a` and `b` are the same, or share a registrable domain. An IP address or a host
/// that is itself a public suffix (`localhost`, say) has none: it shares a site only with itself.
fn same_site(a: &str, b: &str) -> bool {
    let normal = |host: &str| host.trim_end_matches('.').to_ascii_lowercase();
    let (a, b) = (normal(a), normal(b));
    let site = |host: &str| -> Option<String> {
        if host.starts_with('[') || host.parse::<IpAddr>().is_ok() {
            return None;
        }
        psl::domain_str(host).map(str::to_owned)
    };
    !a.is_empty() && (a == b || matches!((site(&a), site(&b)), (Some(x), Some(y)) if x == y))
}

/// A hint was ignored, and the log said so (once per process: every attach of the deployment
/// would say the same).
static IGNORED_LOGGED: AtomicBool = AtomicBool::new(false);

/// Why a request at the read origin is sent again to the stream's origin, which then serves the
/// rest of the attach.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Fallback {
    #[error("no answer: {0}")]
    Transport(#[source] Box<ureq::Error>),
    /// Any answer but a 200, a redirect or a 412 included.
    #[error("status {status}")]
    Status { status: u16 },
    #[error("Stream-Incarnation {found:?}, not the incarnation requested")]
    Incarnation { found: Option<String> },
    #[error("the snapshot's offset {offset:?} or digest {digest:?} is not the one HEAD named")]
    SnapshotMismatch {
        offset: Option<String>,
        digest: Option<String>,
    },
    #[error("HEAD named no snapshot digest")]
    NoDigest,
    #[error("the body broke off: {0}")]
    Body(#[source] Box<ureq::Error>),
    #[error("no usable Stream-Next-Offset")]
    NoNextOffset,
    #[error("{len} bytes from {at} ending at {next}")]
    NotAdvanced {
        at: String,
        len: usize,
        next: String,
    },
    #[error("{0}")]
    Snapshot(#[source] Box<Error>),
}

/// The read origin of one attach, while it is in use.
#[derive(Debug, Default)]
pub(crate) struct ImmutableReads {
    /// The stream's URL at the read origin (`{origin}{path}`), `None` once it is not in use.
    at: Option<String>,
}

impl ImmutableReads {
    /// The read origin of an attach of stream `url`, incarnation `incarnation`, whose `HEAD`
    /// hinted `hint` (see [`chosen`]). None for an incarnation that a URL cannot carry as it is.
    pub(crate) fn for_attach(url: &str, hint: Option<&str>, incarnation: &str) -> ImmutableReads {
        let origin = match chosen(read_origin(), hint, url) {
            Ok(origin) => origin,
            Err(ignored) => {
                if !IGNORED_LOGGED.swap(true, Ordering::Relaxed) {
                    log::emit(Level::Warn, "read_origin_ignored", &[
                        ("stream", &url),
                        ("reason", &ignored),
                    ]);
                }
                None
            }
        };
        match origin {
            Some(origin) if offset_token(incarnation).is_some() => Self::at(&origin, url),
            _ => ImmutableReads::default(),
        }
    }

    /// Reads of stream `url` at `origin`.
    pub(crate) fn at(origin: &Origin, url: &str) -> ImmutableReads {
        let path = url.parse::<Uri>().ok().map(|uri| uri.path().to_owned());
        ImmutableReads {
            at: path.map(|path| format!("{origin}{path}")),
        }
    }

    /// Whether the read origin is still in use.
    #[cfg(test)]
    pub(crate) fn in_use(&self) -> bool {
        self.at.is_some()
    }

    /// The snapshot at `offset` whose digest `HEAD` named (`digest`), from the read origin and
    /// checked by `installable` (offset, size and checksum), with the size of its body. `None` when
    /// the read origin is not in use, or falls back here: fetch it at the stream's origin.
    pub(crate) fn snapshot(
        &mut self,
        url: &str,
        incarnation: &str,
        offset: &str,
        digest: Option<&str>,
        installable: impl Fn(&str, &[u8]) -> Result<Snapshot, Error>,
    ) -> Option<(usize, Snapshot)> {
        let at = self.at.as_deref()?;
        let found = digest
            .ok_or(Fallback::NoDigest)
            .and_then(|digest| get_snapshot_immutable(at, incarnation, offset, digest))
            .and_then(|body| match installable(offset, &body) {
                Ok(snap) => Ok((body.len(), snap)),
                Err(e) => Err(Fallback::Snapshot(Box::new(e))),
            });
        match found {
            Ok(found) => Some(found),
            Err(reason) => {
                self.fall_back(url, &reason);
                None
            }
        }
    }

    /// One aligned read from `pos`, at the read origin: the bytes and the offset after them.
    /// `None` when the read origin is no longer in use (`pos` reached `tail`, `HEAD`'s
    /// `Stream-Next-Offset`, or an earlier answer was not marked immutable), or falls back here:
    /// read at the stream's origin.
    pub(crate) fn read(
        &mut self,
        url: &str,
        incarnation: &str,
        pos: &str,
        tail: Option<&str>,
    ) -> Option<(Vec<u8>, String)> {
        let at = self.at.as_deref()?;
        if tail.is_some_and(|tail| pos >= tail) {
            self.at = None;
            return None;
        }
        match read_immutable(at, incarnation, pos) {
            Ok(read) => {
                if !read.immutable || tail.is_some_and(|tail| read.next.as_str() >= tail) {
                    self.at = None;
                }
                Some((read.bytes, read.next))
            }
            Err(reason) => {
                self.fall_back(url, &reason);
                None
            }
        }
    }

    fn fall_back(&mut self, url: &str, reason: &Fallback) {
        if let Some(at) = self.at.take() {
            log::emit(Level::Warn, "read_origin_fallback", &[
                ("stream", &url),
                ("read_origin", &at),
                ("reason", reason),
            ]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Ignored;
    use super::Origin;
    use super::chosen;
    use super::same_site;
    use crate::config::ReadOriginSetting;

    // An origin is `https://host[:port]` and nothing else: no other scheme, path (a lone `/`
    // included), query, fragment or userinfo.
    #[test]
    fn origins_are_https_with_no_path() {
        let parsed = |v: &str| Origin::parse(v).map(|o| o.to_string());
        assert_eq!(
            parsed("https://reads.example.com"),
            Some("https://reads.example.com".into())
        );
        assert_eq!(
            parsed("HTTPS://Reads.Example.com:8443"),
            Some("https://reads.example.com:8443".into())
        );
        assert_eq!(
            parsed("https://[::1]:443"),
            Some("https://[::1]:443".into())
        );
        for bad in [
            "http://reads.example.com",
            "reads.example.com",
            "https://",
            "https://reads.example.com/",
            "https://reads.example.com/b",
            "https://reads.example.com?x=1",
            "https://reads.example.com#x",
            "https://user@reads.example.com",
            "https://reads.example.com:",
            "https://reads.example.com:x",
            "https://reads .example.com",
            "ftp://reads.example.com",
        ] {
            assert_eq!(parsed(bad), None, "{bad}");
        }
    }

    // The registrable domain decides, from the public suffix list: two hosts under one domain share
    // a site, two under a public suffix (one a deployment's customers share, `github.io` say) do
    // not. An IP address or `localhost` shares a site with itself alone.
    #[test]
    fn sites_are_registrable_domains() {
        assert!(same_site("streams.example.com", "reads.example.com"));
        assert!(same_site("example.com", "cdn.reads.example.com."));
        assert!(same_site("a.example.co.uk", "B.example.co.uk"));
        assert!(!same_site("example.co.uk", "other.co.uk"));
        assert!(!same_site("streams.example.com", "reads.example.net"));
        assert!(!same_site("a.github.io", "b.github.io"));
        assert!(same_site("127.0.0.1", "127.0.0.1"));
        assert!(!same_site("127.0.0.1", "127.0.0.2"));
        assert!(!same_site("localhost", "other.localhost"));
    }

    // Unset, the setting follows a hint that is an https origin within the stream's site. `off`
    // follows none, and a configured origin is used whatever the hint says, or without one.
    #[test]
    fn hints_are_followed_within_the_site_unless_configured() {
        let url = "https://streams.example.com/b/s";
        let hint = ReadOriginSetting::Hint;
        let ok = |o: &str| Ok(Some(Origin::parse(o).unwrap()));
        assert_eq!(
            chosen(&hint, Some("https://reads.example.com"), url),
            ok("https://reads.example.com")
        );
        assert_eq!(chosen(&hint, None, url), Ok(None));
        assert_eq!(
            chosen(&hint, Some("http://reads.example.com"), url),
            Err(Ignored::NotHttpsOrigin {
                hint: "http://reads.example.com".into()
            })
        );
        assert_eq!(
            chosen(&hint, Some("https://reads.example.net"), url),
            Err(Ignored::OtherSite {
                hint: "https://reads.example.net".into(),
                stream: "streams.example.com".into()
            })
        );
        let off = ReadOriginSetting::Off;
        assert_eq!(
            chosen(&off, Some("https://reads.example.com"), url),
            Ok(None)
        );
        let configured =
            ReadOriginSetting::Configured(Origin::parse("https://cdn.elsewhere.net").unwrap());
        assert_eq!(
            chosen(&configured, Some("https://reads.example.com"), url),
            ok("https://cdn.elsewhere.net")
        );
        assert_eq!(
            chosen(&configured, None, url),
            ok("https://cdn.elsewhere.net")
        );
    }
}
