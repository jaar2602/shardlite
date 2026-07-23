//! The two cryptographic jobs the console needs, and nothing more.
//!
//! - **Console passwords** are hashed with Argon2id (memory-hard), stored as PHC strings, and
//!   verified in constant time by the library. The plaintext is never stored.
//! - **Stored shardlite secrets** are sealed with ChaCha20-Poly1305 under a key derived from a
//!   console master passphrase. The registry file alone therefore grants no cluster access —
//!   an attacker needs the file *and* the passphrase (which lives in the environment, not on
//!   disk). This is scoping decision 3: encrypted at rest.

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::rngs::OsRng;
use rand::RngCore;

/// Hash a console password for storage (PHC string, salt embedded).
pub fn hash_password(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .expect("argon2 hashing cannot fail on a valid password")
        .to_string()
}

/// Verify a password against a stored PHC hash. A malformed stored hash verifies as `false`
/// rather than panicking — a corrupt record must not become a crash.
pub fn verify_password(password: &str, phc: &str) -> bool {
    match PasswordHash::new(phc) {
        Ok(parsed) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok(),
        Err(_) => false,
    }
}

/// Authenticated encryption for stored secrets, keyed by the console master passphrase.
pub struct Sealer {
    cipher: ChaCha20Poly1305,
    legacy_cipher: ChaCha20Poly1305,
}

impl Sealer {
    /// Derive the encryption key from the operator's master passphrase with Argon2id. The legacy
    /// BLAKE3-derived key remains available only to read v1 ciphertext; every new seal is v2.
    pub fn from_passphrase(passphrase: &str) -> Self {
        let mut key_bytes = [0u8; 32];
        Argon2::default()
            .hash_password_into(
                passphrase.as_bytes(),
                b"shardlite-console-v2-registry",
                &mut key_bytes,
            )
            .expect("fixed Argon2id key derivation parameters are valid");
        let legacy_bytes = blake3::derive_key(
            "shardlite-console 2026 registry secret encryption key",
            passphrase.as_bytes(),
        );
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key_bytes));
        let legacy_cipher = ChaCha20Poly1305::new(Key::from_slice(&legacy_bytes));
        Self {
            cipher,
            legacy_cipher,
        }
    }

    /// Encrypt `plaintext`, returning base64 of `nonce (12 bytes) || ciphertext+tag`. A fresh
    /// random nonce per call means encrypting the same secret twice yields different blobs.
    pub fn seal(&self, plaintext: &[u8]) -> String {
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);
        let ciphertext = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .expect("ChaCha20-Poly1305 encryption cannot fail");
        let mut blob = nonce.to_vec();
        blob.extend_from_slice(&ciphertext);
        format!("v2:{}", B64.encode(blob))
    }

    /// Reverse [`seal`]. Returns `None` on a bad key, tampering, or a malformed blob — the
    /// authentication tag makes "wrong passphrase" and "corrupted file" indistinguishable from
    /// "not a valid secret", which is exactly right: all three must fail closed.
    pub fn open(&self, sealed: &str) -> Option<Vec<u8>> {
        let (cipher, encoded) = match sealed.strip_prefix("v2:") {
            Some(encoded) => (&self.cipher, encoded),
            None => (&self.legacy_cipher, sealed),
        };
        let blob = B64.decode(encoded).ok()?;
        if blob.len() < 12 {
            return None;
        }
        let (nonce, ciphertext) = blob.split_at(12);
        cipher.decrypt(Nonce::from_slice(nonce), ciphertext).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_verifies_only_against_itself() {
        let hash = hash_password("correct horse");
        assert!(verify_password("correct horse", &hash));
        assert!(!verify_password("Correct Horse", &hash));
        assert!(!verify_password("", &hash));
    }

    #[test]
    fn a_malformed_hash_fails_closed_not_panics() {
        assert!(!verify_password("anything", "not-a-phc-string"));
    }

    #[test]
    fn a_sealed_secret_round_trips_only_with_the_right_passphrase() {
        let sealer = Sealer::from_passphrase("master-key");
        let blob = sealer.seal(b"cluster-secret");
        assert_eq!(sealer.open(&blob).as_deref(), Some(&b"cluster-secret"[..]));

        // A different passphrase derives a different key; the tag rejects it.
        let wrong = Sealer::from_passphrase("other-key");
        assert_eq!(wrong.open(&blob), None);
    }

    #[test]
    fn the_same_secret_seals_to_different_blobs() {
        let sealer = Sealer::from_passphrase("k");
        assert_ne!(sealer.seal(b"x"), sealer.seal(b"x"));
    }

    #[test]
    fn v1_ciphertext_remains_readable_during_migration() {
        let passphrase = "master-key";
        let key = blake3::derive_key(
            "shardlite-console 2026 registry secret encryption key",
            passphrase.as_bytes(),
        );
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        let nonce = [7u8; 12];
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce), b"old-secret".as_ref())
            .unwrap();
        let mut blob = nonce.to_vec();
        blob.extend(ciphertext);
        let legacy = B64.encode(blob);

        assert_eq!(
            Sealer::from_passphrase(passphrase).open(&legacy).as_deref(),
            Some(&b"old-secret"[..])
        );
    }
}
