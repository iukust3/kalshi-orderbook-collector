//! Request signing for Kalshi.
//!
//! Every REST call and the websocket handshake are authenticated with a signature over
//! `"{timestamp_ms}{method}{path}"`. The signature travels in
//! `KALSHI-ACCESS-SIGNATURE`, alongside the key id and the same millisecond timestamp.
//!
//! Two things are easy to get wrong and both are handled here:
//!
//! * **The signed path is the endpoint path, not the URL.** Query parameters are excluded,
//!   so `/trade-api/v2/markets?status=open` signs as `/trade-api/v2/markets`. The websocket
//!   handshake therefore signs `/trade-api/ws/v2`.
//! * **Key type is not visible in the PEM header.** Kalshi issues either Ed25519 keys
//!   (PKCS#8, `BEGIN PRIVATE KEY`) or RSA-2048 keys (PKCS#1, `BEGIN RSA PRIVATE KEY`), but
//!   an RSA key regenerated with `openssl genpkey` is also PKCS#8, so the header proves
//!   nothing. The key is parsed, and the signing algorithm follows what was parsed:
//!   Ed25519 signs the message directly, RSA uses RSA-PSS over SHA-256 with a salt as long
//!   as the digest. Picking the wrong one produces a 401 with no useful detail, which is
//!   why [`KalshiSigner::from_pem`] sniffs instead of trusting the filename.

use anyhow::Context;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::Signer;
use ed25519_dalek::pkcs8::DecodePrivateKey as DecodeEd25519;
use rand_core::OsRng;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pss::SigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use sha2::Sha256;

pub const HEADER_KEY: &str = "KALSHI-ACCESS-KEY";
pub const HEADER_SIGNATURE: &str = "KALSHI-ACCESS-SIGNATURE";
pub const HEADER_TIMESTAMP: &str = "KALSHI-ACCESS-TIMESTAMP";

enum Key {
    Rsa(Box<SigningKey<Sha256>>),
    Ed25519(Box<ed25519_dalek::SigningKey>),
}

pub struct KalshiSigner {
    key_id: String,
    key: Key,
}

impl KalshiSigner {
    /// Parses whichever key type the PEM holds. Ed25519 is tried first because its PKCS#8
    /// decoder rejects an RSA key outright, whereas an RSA decoder would happily consume a
    /// PKCS#8 wrapper and mis-sign.
    pub fn from_pem(key_id: impl Into<String>, pem: &str) -> anyhow::Result<Self> {
        let key = if let Ok(ed25519) = ed25519_dalek::SigningKey::from_pkcs8_pem(pem) {
            Key::Ed25519(Box::new(ed25519))
        } else {
            let rsa_key = rsa::RsaPrivateKey::from_pkcs8_pem(pem)
                .or_else(|_| rsa::RsaPrivateKey::from_pkcs1_pem(pem))
                .context(
                    "KALSHI_PRIVATE_KEY_PATH must contain a PKCS#8 Ed25519 key, \
                     a PKCS#8 RSA key, or a PKCS#1 RSA key in PEM format",
                )?;
            Key::Rsa(Box::new(SigningKey::<Sha256>::new(rsa_key)))
        };
        Ok(Self {
            key_id: key_id.into(),
            key,
        })
    }

    pub fn from_file(
        key_id: impl Into<String>,
        path: impl AsRef<std::path::Path>,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let pem = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read Kalshi private key at {}", path.display()))?;
        Self::from_pem(key_id, &pem)
    }

    /// Headers for one request. `path` excludes the host and any query string, e.g.
    /// `/trade-api/v2/markets`.
    pub fn headers(&self, method: &str, path: &str) -> anyhow::Result<Vec<(&'static str, String)>> {
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let signed_path = path.split('?').next().unwrap_or(path);
        let payload = format!("{timestamp}{}{signed_path}", method.to_ascii_uppercase());

        let signature = match &self.key {
            Key::Rsa(key) => {
                let signature = key.sign_with_rng(&mut OsRng, payload.as_bytes());
                BASE64.encode(signature.to_bytes())
            }
            Key::Ed25519(key) => BASE64.encode(key.sign(payload.as_bytes()).to_bytes()),
        };

        Ok(vec![
            (HEADER_KEY, self.key_id.clone()),
            (HEADER_SIGNATURE, signature),
            (HEADER_TIMESTAMP, timestamp),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs8::EncodePrivateKey;

    #[test]
    fn rsa_key_signs_and_drops_the_query_string() {
        let key = rsa::RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let pem = key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string();
        let signer = KalshiSigner::from_pem("key-id", &pem).unwrap();
        let headers = signer
            .headers("GET", "/trade-api/v2/markets?status=open&limit=1000")
            .unwrap();
        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0].1, "key-id");
        assert!(!headers[1].1.is_empty());
        assert!(headers[2].1.parse::<i64>().is_ok());
    }

    #[test]
    fn rsa_pkcs1_key_is_accepted() {
        // Kalshi's own RSA download is PKCS#1 (`BEGIN RSA PRIVATE KEY`), which the PKCS#8
        // decoder rejects. Generating one here proves the fallback path is wired.
        use rsa::pkcs1::EncodeRsaPrivateKey;
        let key = rsa::RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let pem = key
            .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
            .unwrap()
            .to_string();
        assert!(pem.contains("BEGIN RSA PRIVATE KEY"));
        let signer = KalshiSigner::from_pem("key-id", &pem).unwrap();
        assert_eq!(signer.headers("GET", "/trade-api/ws/v2").unwrap().len(), 3);
    }

    #[test]
    fn ed25519_key_is_detected_from_pkcs8() {
        use ed25519_dalek::pkcs8::EncodePrivateKey as EncodeEd25519;
        let key = ed25519_dalek::SigningKey::generate(&mut OsRng);
        let pem = key
            .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
            .unwrap()
            .to_string();
        // The header is the PKCS#8 one, so the signature is the only proof of key type.
        assert!(pem.contains("BEGIN PRIVATE KEY"));
        let signer = KalshiSigner::from_pem("key-id", &pem).unwrap();
        let headers = signer.headers("GET", "/trade-api/v2/markets").unwrap();
        // 64 raw bytes, base64 encoded.
        assert_eq!(BASE64.decode(&headers[1].1).unwrap().len(), 64);
    }
}
