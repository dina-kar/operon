//! Environments the desktop can talk to (AP1 Task 5).
//!
//! `local` is the bundled sidecar on loopback (no credentials before the
//! auth plan, D111). `apps-mock` is `loams-apps-mock` on 127.0.0.1:8084, for
//! development. A remote environment (an HTTPS Loams instance with Authentik
//! sign-in) comes from `LOAMS_DESKTOP_REMOTE` in this scaffold; profiles in
//! the app config directory follow (AP1 Task 7).

use serde::Serialize;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvKind {
    /// Loopback only; requests carry no credentials (D111).
    Local,
    /// HTTPS only; requests carry the environment's bearer token.
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Environment {
    pub id: String,
    pub name: String,
    pub kind: EnvKind,
    /// The Connect base URL; `None` until a local sidecar is ready.
    pub base_url: Option<Url>,
}

impl Environment {
    /// The origin requests may go to, as `scheme://host:port`.
    #[must_use]
    pub fn origin(&self) -> Option<url::Origin> {
        self.base_url.as_ref().map(Url::origin)
    }
}

#[derive(Debug, Clone)]
pub struct Envs {
    list: Vec<Environment>,
    active: Option<String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EnvError {
    #[error("no environment `{0}`")]
    Unknown(String),
    #[error("a remote environment must use https: {0}")]
    InsecureRemote(String),
}

pub const LOCAL: &str = "local";
pub const APPS_MOCK: &str = "apps-mock";
pub const REMOTE: &str = "remote";

impl Envs {
    /// The built-in environments, and a remote one from `remote` if given.
    ///
    /// # Errors
    ///
    /// A remote URL that is not `https`.
    pub fn new(remote: Option<&str>) -> Result<Self, EnvError> {
        let mut list = vec![
            Environment {
                id: LOCAL.into(),
                name: "This computer".into(),
                kind: EnvKind::Local,
                base_url: None,
            },
            Environment {
                id: APPS_MOCK.into(),
                name: "loams-apps-mock (development)".into(),
                kind: EnvKind::Local,
                base_url: Url::parse("http://127.0.0.1:8084/").ok(),
            },
        ];
        if let Some(remote) = remote {
            let url = Url::parse(remote).map_err(|_| EnvError::InsecureRemote(remote.into()))?;
            if url.scheme() != "https" {
                return Err(EnvError::InsecureRemote(remote.into()));
            }
            list.push(Environment {
                id: REMOTE.into(),
                name: url.host_str().unwrap_or("remote").into(),
                kind: EnvKind::Remote,
                base_url: Some(url),
            });
        }
        Ok(Self { list, active: None })
    }

    #[must_use]
    pub fn list(&self) -> &[Environment] {
        &self.list
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<&Environment> {
        self.list.iter().find(|e| e.id == id)
    }

    #[must_use]
    pub fn active(&self) -> Option<&Environment> {
        self.active.as_deref().and_then(|id| self.get(id))
    }

    /// Makes `id` the environment the console and the bridge use.
    ///
    /// # Errors
    ///
    /// An unknown id.
    pub fn select(&mut self, id: &str) -> Result<&Environment, EnvError> {
        if self.get(id).is_none() {
            return Err(EnvError::Unknown(id.into()));
        }
        self.active = Some(id.into());
        self.get(id).ok_or_else(|| EnvError::Unknown(id.into()))
    }

    /// Records the local sidecar's URL once it is ready (or `None` when it stops).
    pub fn set_local(&mut self, url: Option<Url>) {
        if let Some(local) = self.list.iter_mut().find(|e| e.id == LOCAL) {
            local.base_url = url;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_environments_must_be_https() {
        assert_eq!(
            Envs::new(Some("http://loams.example")).unwrap_err(),
            EnvError::InsecureRemote("http://loams.example".into())
        );
        let envs = Envs::new(Some("https://loams.example")).unwrap();
        assert_eq!(envs.get(REMOTE).unwrap().kind, EnvKind::Remote);
    }

    #[test]
    fn select_and_local_url() {
        let mut envs = Envs::new(None).unwrap();
        assert!(envs.active().is_none());
        assert_eq!(
            envs.select("nope").unwrap_err(),
            EnvError::Unknown("nope".into())
        );
        envs.select(LOCAL).unwrap();
        assert!(envs.active().unwrap().base_url.is_none());
        envs.set_local(Url::parse("http://127.0.0.1:49152/").ok());
        assert_eq!(
            envs.active().unwrap().base_url.as_ref().map(Url::as_str),
            Some("http://127.0.0.1:49152/")
        );
    }
}
