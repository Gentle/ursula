//! Shared shutdown-signal handling and graceful HTTP serving for Ursula
//! binaries.
//!
//! Every Ursula binary translates SIGTERM (systemd stop, Kubernetes pod
//! termination) and Ctrl-C into the same graceful-drain dance; this module
//! keeps that plumbing in one place. Binaries that need extra policy (forced
//! exit after a grace period, multiple listeners) compose these primitives
//! instead of re-rolling signal handling.

use std::future::Future;
use std::future::IntoFuture;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use axum::serve::Listener;
use axum::serve::ListenerExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

/// Wait for a termination signal: SIGTERM or Ctrl-C (Ctrl-C only on
/// non-unix). If the SIGTERM handler cannot be installed, falls back to
/// Ctrl-C alone instead of failing.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("received Ctrl+C, starting graceful shutdown");
                    }
                    _ = sigterm.recv() => {
                        tracing::info!("received SIGTERM, starting graceful shutdown");
                    }
                }
            }
            Err(err) => {
                tracing::error!("install SIGTERM handler: {err}; falling back to Ctrl-C only");
                if let Err(err) = tokio::signal::ctrl_c().await {
                    // Without any signal handler, only process exit stops the server.
                    tracing::error!(
                        "install Ctrl-C handler: {err}; graceful shutdown is unavailable"
                    );
                    std::future::pending::<()>().await;
                }
                tracing::info!("received Ctrl+C, starting graceful shutdown");
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(err) = tokio::signal::ctrl_c().await {
            // Without any signal handler, only process exit stops the server.
            tracing::error!("install Ctrl-C handler: {err}; graceful shutdown is unavailable");
            std::future::pending::<()>().await;
        }
        tracing::info!("received Ctrl+C, starting graceful shutdown");
    }
}

/// Wrap `listener` so that every socket it accepts sets `TCP_NODELAY`.
///
/// Without it, Nagle's algorithm holds a reply while an earlier reply on the
/// same connection is unacknowledged, and the peer delays that
/// acknowledgement (at least 40 ms on Linux) until its next request. A Raft
/// follower that writes two replies in a row then answers each ReadIndex
/// round one round late (#521, #547).
pub fn nodelay_listener(listener: TcpListener) -> impl Listener<Io = TcpStream, Addr = SocketAddr> {
    listener.tap_io(|stream| {
        if let Err(err) = stream.set_nodelay(true) {
            tracing::warn!("set TCP_NODELAY on accepted socket: {err}");
        }
    })
}

/// Serve `listener` with `app` until `shutdown` completes, then stop accepting
/// new connections and drain in-flight requests before returning.
///
/// `drain_timeout` bounds the drain: with `Some(t)` the server returns `Ok`
/// after `t` even if connections are still open; with `None` the drain is
/// unbounded (callers enforce their own deadline, e.g. via a forced-exit
/// watchdog).
pub async fn serve_until_shutdown(
    listener: TcpListener,
    app: axum::Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
    drain_timeout: Option<Duration>,
) -> io::Result<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(nodelay_listener(listener), app)
        .with_graceful_shutdown(async move {
            drop(shutdown_rx.await);
        })
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => result,
        () = shutdown => {
            if let Some(timeout) = drain_timeout {
                tracing::info!(
                    drain_timeout_secs = timeout.as_secs(),
                    "graceful shutdown initiated"
                );
            }
            if shutdown_tx.send(()).is_err() {
                tracing::debug!("graceful shutdown receiver already dropped");
            }
            match drain_timeout {
                None => server.await,
                Some(timeout) => match tokio::time::timeout(timeout, &mut server).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::warn!(
                            drain_timeout_secs = timeout.as_secs(),
                            "graceful drain timed out; abandoning the connections still open"
                        );
                        Ok(())
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::serve::Listener;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;

    use super::nodelay_listener;

    /// A connection accepted through [`nodelay_listener`] has `TCP_NODELAY`
    /// set, so `serve_until_shutdown` serves every connection with it.
    #[tokio::test]
    async fn nodelay_listener_sets_nodelay_on_accepted_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut listener = nodelay_listener(listener);
        let ((accepted, _peer), client) =
            tokio::join!(listener.accept(), TcpStream::connect(address));
        client.unwrap();
        assert!(accepted.nodelay().unwrap());
    }
}
