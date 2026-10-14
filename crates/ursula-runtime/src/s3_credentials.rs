//! AWS container credentials (ECS task role / EKS Pod Identity) for opendal S3.
//!
//! opendal 0.51 signs with reqsign 0.16, whose default AWS loader only knows
//! static keys, web identity and EC2 IMDSv2. On ECS/Fargate there is no IMDS,
//! so cold-store requests fail with `loading credential to sign http request`.
//! The container credentials provider is selected here instead, when the
//! container runtime advertised it (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or
//! `AWS_CONTAINER_CREDENTIALS_FULL_URI`) and no static keys are configured.
//!
//! opendal calls the loader for every signed request and does not cache, so the
//! loader caches the credential itself and refetches once it is within
//! reqsign's validity buffer (2 minutes) of `Expiration`. ECS rotates task-role
//! credentials well before that, so a refetch always gets a fresh set.

use std::path::PathBuf;
use std::time::Duration;

use reqsign::AwsCredential;
use reqsign::AwsCredentialLoad;
use reqwest::Client;
use reqwest::Url;
use serde::Deserialize;

use crate::rt::sync::Mutex;

const RELATIVE_URI_ENV: &str = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
const FULL_URI_ENV: &str = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
const AUTH_TOKEN_ENV: &str = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
const AUTH_TOKEN_FILE_ENV: &str = "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE";
/// The ECS agent's credentials endpoint `RELATIVE_URI` is resolved against.
const ECS_CREDENTIALS_BASE: &str = "http://169.254.170.2";
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// Installs the container credentials loader on `builder` when the process
/// runs in a container that advertises one and `has_static_keys` is false
/// (static keys, from config or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`,
/// keep reqsign's default loader). Otherwise returns `builder` unchanged.
pub fn with_container_credentials(
    builder: opendal::services::S3,
    has_static_keys: bool,
) -> opendal::services::S3 {
    match container_credentials_source(
        has_static_keys,
        |name| std::env::var(name).ok(),
        ECS_CREDENTIALS_BASE,
    ) {
        Some(source) => {
            tracing::info!(url = %source.url, "s3: using AWS container credentials provider");
            builder.customized_credential_load(Box::new(ContainerCredentialLoader::new(source)))
        }
        None => builder,
    }
}

/// Where and how to fetch container credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ContainerCredentialsSource {
    url: String,
    auth: Option<ContainerAuth>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ContainerAuth {
    Token(String),
    /// Re-read on every fetch: EKS Pod Identity rotates the file.
    TokenFile(PathBuf),
}

/// Resolves the container credentials source from `env` (env vars passed in
/// so tests need not touch the process environment). `RELATIVE_URI` wins over
/// `FULL_URI`, and a token file over an inline token, as in the AWS SDKs.
fn container_credentials_source(
    has_static_keys: bool,
    env: impl Fn(&str) -> Option<String>,
    relative_base: &str,
) -> Option<ContainerCredentialsSource> {
    let env = |name: &str| env(name).filter(|value| !value.trim().is_empty());
    if has_static_keys
        || (env("AWS_ACCESS_KEY_ID").is_some() && env("AWS_SECRET_ACCESS_KEY").is_some())
    {
        return None;
    }
    let url = if let Some(relative) = env(RELATIVE_URI_ENV) {
        format!("{relative_base}{relative}")
    } else {
        env(FULL_URI_ENV)?
    };
    let auth = env(AUTH_TOKEN_FILE_ENV)
        .map(|path| ContainerAuth::TokenFile(PathBuf::from(path)))
        .or_else(|| env(AUTH_TOKEN_ENV).map(ContainerAuth::Token));
    Some(ContainerCredentialsSource { url, auth })
}

/// reqsign credential loader backed by the container credentials endpoint.
struct ContainerCredentialLoader {
    source: ContainerCredentialsSource,
    cached: Mutex<Option<AwsCredential>>,
}

impl ContainerCredentialLoader {
    fn new(source: ContainerCredentialsSource) -> Self {
        Self {
            source,
            cached: Mutex::new(None),
        }
    }

    async fn fetch(&self, client: &Client) -> anyhow::Result<AwsCredential> {
        let url = Url::parse(&self.source.url)
            .map_err(|err| anyhow::anyhow!("invalid container credentials url: {err}"))?;
        let mut request = client.get(url).timeout(FETCH_TIMEOUT);
        let token = match &self.source.auth {
            None => None,
            Some(ContainerAuth::Token(token)) => Some(token.clone()),
            Some(ContainerAuth::TokenFile(path)) => Some(
                tokio::fs::read_to_string(path)
                    .await
                    .map_err(|err| anyhow::anyhow!("read {}: {err}", path.display()))?,
            ),
        };
        if let Some(token) = token {
            request = request.header(reqwest::header::AUTHORIZATION, token.trim());
        }
        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            anyhow::bail!("container credentials endpoint returned {status}: {body}");
        }
        parse_credentials(&body)
    }
}

#[async_trait::async_trait]
impl AwsCredentialLoad for ContainerCredentialLoader {
    async fn load_credential(&self, client: Client) -> anyhow::Result<Option<AwsCredential>> {
        // Held across the fetch so concurrent signers share one refresh.
        let mut cached = self.cached.lock().await;
        if let Some(credential) = cached.as_ref().filter(|cred| cred.is_valid()) {
            return Ok(Some(credential.clone()));
        }
        match self.fetch(&client).await {
            Ok(credential) => {
                *cached = Some(credential.clone());
                Ok(Some(credential))
            }
            // Inside the refresh buffer but not yet expired: keep signing with
            // the old set rather than failing the request.
            Err(err) => match cached.as_ref() {
                Some(credential)
                    if credential
                        .expires_in
                        .is_some_and(|expires| expires > chrono::Utc::now()) =>
                {
                    tracing::warn!(error = %err, "s3: container credentials refresh failed; reusing unexpired credential");
                    Ok(Some(credential.clone()))
                }
                _ => Err(err),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ContainerCredentialsResponse {
    access_key_id: String,
    secret_access_key: String,
    token: Option<String>,
    expiration: Option<String>,
}

fn parse_credentials(body: &str) -> anyhow::Result<AwsCredential> {
    let response: ContainerCredentialsResponse = serde_json::from_str(body)
        .map_err(|err| anyhow::anyhow!("parse container credentials response: {err}"))?;
    let expires_in = response
        .expiration
        .map(|expiration| {
            chrono::DateTime::parse_from_rfc3339(&expiration)
                .map(|at| at.with_timezone(&chrono::Utc))
                .map_err(|err| anyhow::anyhow!("parse Expiration {expiration:?}: {err}"))
        })
        .transpose()?;
    Ok(AwsCredential {
        access_key_id: response.access_key_id,
        secret_access_key: response.secret_access_key,
        session_token: response.token,
        expires_in,
    })
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::*;

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        move |name| vars.get(name).cloned()
    }

    fn credentials_json(expiration: chrono::DateTime<chrono::Utc>) -> String {
        format!(
            r#"{{"RoleArn":"arn:aws:iam::1:role/task","AccessKeyId":"AKIA","SecretAccessKey":"secret","Token":"session","Expiration":"{}"}}"#,
            expiration.to_rfc3339()
        )
    }

    /// One-response-per-connection HTTP stub. Records each request head and
    /// answers with `(status, body)`.
    async fn stub_server(
        status: u16,
        body: String,
    ) -> (String, Arc<std::sync::Mutex<Vec<String>>>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hits = Arc::new(AtomicUsize::new(0));
        let (requests_task, hits_task) = (requests.clone(), hits.clone());
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0; 8192];
                let mut read = 0;
                while !buf[..read].windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = socket.read(&mut buf[read..]).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    read += n;
                }
                requests_task
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf[..read]).into_owned());
                hits_task.fetch_add(1, Ordering::SeqCst);
                let response = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        (base, requests, hits)
    }

    #[test]
    fn selected_only_with_container_env_and_no_static_keys() {
        let relative = env(&[(RELATIVE_URI_ENV, "/v2/credentials/abc")]);
        assert_eq!(
            container_credentials_source(false, &relative, "http://base"),
            Some(ContainerCredentialsSource {
                url: "http://base/v2/credentials/abc".into(),
                auth: None,
            })
        );
        assert_eq!(
            container_credentials_source(true, &relative, "http://base"),
            None
        );
        assert_eq!(
            container_credentials_source(false, env(&[]), "http://base"),
            None
        );
        assert_eq!(
            container_credentials_source(false, env(&[(RELATIVE_URI_ENV, " ")]), "http://base"),
            None
        );
        let env_keys = env(&[
            (RELATIVE_URI_ENV, "/creds"),
            ("AWS_ACCESS_KEY_ID", "AKIA"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
        ]);
        assert_eq!(
            container_credentials_source(false, env_keys, "http://base"),
            None
        );
    }

    #[test]
    fn full_uri_and_auth_precedence() {
        let full = env(&[
            (FULL_URI_ENV, "http://127.0.0.1:9/creds"),
            (AUTH_TOKEN_ENV, "tok"),
        ]);
        assert_eq!(
            container_credentials_source(false, full, "http://base"),
            Some(ContainerCredentialsSource {
                url: "http://127.0.0.1:9/creds".into(),
                auth: Some(ContainerAuth::Token("tok".into())),
            })
        );
        let both = env(&[
            (RELATIVE_URI_ENV, "/rel"),
            (FULL_URI_ENV, "http://127.0.0.1:9/creds"),
            (AUTH_TOKEN_ENV, "tok"),
            (AUTH_TOKEN_FILE_ENV, "/var/run/token"),
        ]);
        assert_eq!(
            container_credentials_source(false, both, "http://base"),
            Some(ContainerCredentialsSource {
                url: "http://base/rel".into(),
                auth: Some(ContainerAuth::TokenFile("/var/run/token".into())),
            })
        );
    }

    #[tokio::test]
    async fn loads_relative_uri_credentials_and_caches_until_expiry() {
        let expiration = chrono::Utc::now() + chrono::TimeDelta::hours(1);
        let (base, requests, hits) = stub_server(200, credentials_json(expiration)).await;
        let source = container_credentials_source(
            false,
            env(&[(RELATIVE_URI_ENV, "/v2/credentials/abc")]),
            &base,
        )
        .unwrap();
        let loader = ContainerCredentialLoader::new(source);

        let cred = loader
            .load_credential(Client::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cred.access_key_id, "AKIA");
        assert_eq!(cred.secret_access_key, "secret");
        assert_eq!(cred.session_token.as_deref(), Some("session"));
        assert_eq!(cred.expires_in.unwrap().timestamp(), expiration.timestamp());
        assert!(requests.lock().unwrap()[0].starts_with("GET /v2/credentials/abc HTTP/1.1"));

        loader
            .load_credential(Client::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1, "valid credential is cached");
    }

    #[tokio::test]
    async fn refetches_credential_inside_expiry_buffer() {
        // Expires in 1 minute: inside reqsign's 2-minute buffer, so never valid.
        let expiration = chrono::Utc::now() + chrono::TimeDelta::minutes(1);
        let (base, _requests, hits) = stub_server(200, credentials_json(expiration)).await;
        let loader = ContainerCredentialLoader::new(ContainerCredentialsSource {
            url: format!("{base}/creds"),
            auth: None,
        });
        loader
            .load_credential(Client::new())
            .await
            .unwrap()
            .unwrap();
        loader
            .load_credential(Client::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn full_uri_sends_authorization_token() {
        let expiration = chrono::Utc::now() + chrono::TimeDelta::hours(1);
        let (base, requests, _hits) = stub_server(200, credentials_json(expiration)).await;
        let token_dir = tempfile::tempdir().unwrap();
        let token_file = token_dir.path().join("token");
        std::fs::write(&token_file, "file-token\n").unwrap();

        let inline = container_credentials_source(
            false,
            env(&[
                (FULL_URI_ENV, &format!("{base}/full")),
                (AUTH_TOKEN_ENV, "inline-token"),
            ]),
            "http://unused",
        )
        .unwrap();
        ContainerCredentialLoader::new(inline)
            .load_credential(Client::new())
            .await
            .unwrap()
            .unwrap();
        let from_file = container_credentials_source(
            false,
            env(&[
                (FULL_URI_ENV, &format!("{base}/full")),
                (AUTH_TOKEN_FILE_ENV, token_file.to_str().unwrap()),
            ]),
            "http://unused",
        )
        .unwrap();
        ContainerCredentialLoader::new(from_file)
            .load_credential(Client::new())
            .await
            .unwrap()
            .unwrap();

        let requests = requests.lock().unwrap();
        assert!(requests[0].starts_with("GET /full HTTP/1.1"));
        assert!(
            requests[0]
                .to_ascii_lowercase()
                .contains("authorization: inline-token\r\n")
        );
        assert!(
            requests[1]
                .to_ascii_lowercase()
                .contains("authorization: file-token\r\n")
        );
    }

    #[tokio::test]
    async fn non_200_is_an_error() {
        let (base, _requests, _hits) = stub_server(403, "denied".into()).await;
        let loader = ContainerCredentialLoader::new(ContainerCredentialsSource {
            url: format!("{base}/creds"),
            auth: None,
        });
        let Err(err) = loader.load_credential(Client::new()).await else {
            panic!("non-200 must fail");
        };
        assert!(err.to_string().contains("403"), "{err}");
    }

    #[tokio::test]
    async fn opendal_signs_with_container_credentials() {
        let expiration = chrono::Utc::now() + chrono::TimeDelta::hours(1);
        let (creds_base, _requests, _hits) = stub_server(200, credentials_json(expiration)).await;
        let (s3_base, s3_requests, _hits) = stub_server(404, String::new()).await;
        let builder = opendal::services::S3::default()
            .bucket("bucket")
            .region("us-east-1")
            .endpoint(&s3_base)
            .customized_credential_load(Box::new(ContainerCredentialLoader::new(
                ContainerCredentialsSource {
                    url: format!("{creds_base}/creds"),
                    auth: None,
                },
            )));
        let operator = opendal::Operator::new(builder).unwrap().finish();
        let err = operator.stat("missing").await.unwrap_err();
        assert_eq!(err.kind(), opendal::ErrorKind::NotFound, "{err}");
        let request = s3_requests.lock().unwrap()[0].to_ascii_lowercase();
        assert!(request.contains("credential=akia/"), "{request}");
        assert!(
            request.contains("x-amz-security-token: session"),
            "{request}"
        );
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(parse_credentials("{}").is_err());
        assert!(
            parse_credentials(r#"{"AccessKeyId":"a","SecretAccessKey":"b","Expiration":"nope"}"#)
                .is_err()
        );
    }
}
