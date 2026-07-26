//! Deterministic crash injection for restart qualification.
//!
//! Production builds compile [`hit`] to a no-op. A binary built with the `failpoints` feature
//! exits at an exact named boundary when `SHARDLITE_FAILPOINT` contains that name. The optional
//! `SHARDLITE_FAILPOINT_MARKER` makes the crash one-shot across process restarts: the first process
//! creates and fsyncs the marker before exiting; later processes see it and continue.
//!
//! This deliberately uses `process::exit`, not a panic. Panics run destructors while a real crash
//! does not, and therefore make an optimistic recovery test.

/// Exit status used by an injected deterministic crash.
pub const EXIT_CODE: i32 = 86;

/// Crash at `name` when this build and process have enabled that failpoint.
#[inline]
pub fn hit(name: &'static str) {
    #[cfg(feature = "failpoints")]
    enabled::hit(name);

    #[cfg(not(feature = "failpoints"))]
    let _ = name;
}

#[cfg(feature = "failpoints")]
mod enabled {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::Path;

    const SPEC_ENV: &str = "SHARDLITE_FAILPOINT";
    const MARKER_ENV: &str = "SHARDLITE_FAILPOINT_MARKER";

    pub(super) fn hit(name: &'static str) {
        let Ok(spec) = std::env::var(SPEC_ENV) else {
            return;
        };
        if !matches(&spec, name) {
            return;
        }
        if let Ok(marker) = std::env::var(MARKER_ENV)
            && !claim_once(Path::new(&marker), name)
        {
            return;
        }

        eprintln!("SHARDLITE_FAILPOINT_HIT {name}");
        std::process::exit(super::EXIT_CODE);
    }

    fn matches(spec: &str, name: &str) -> bool {
        spec.split(',')
            .map(str::trim)
            .any(|candidate| candidate == "*" || candidate == name)
    }

    fn claim_once(path: &Path, name: &str) -> bool {
        if let Some(parent) = path.parent()
            && std::fs::create_dir_all(parent).is_err()
        {
            // A broken qualification setup must fail closed: still crash, and make the missing
            // marker visible in stderr rather than silently skipping the requested boundary.
            eprintln!(
                "SHARDLITE_FAILPOINT_MARKER_ERROR {}: could not create parent",
                path.display()
            );
            return true;
        }
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                if writeln!(file, "{name}")
                    .and_then(|()| file.sync_all())
                    .is_err()
                {
                    eprintln!(
                        "SHARDLITE_FAILPOINT_MARKER_ERROR {}: could not persist marker",
                        path.display()
                    );
                }
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(error) => {
                eprintln!(
                    "SHARDLITE_FAILPOINT_MARKER_ERROR {}: {error}",
                    path.display()
                );
                true
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::matches;

        #[test]
        fn exact_comma_separated_names_and_wildcard_match() {
            assert!(matches("split.after_install", "split.after_install"));
            assert!(matches(
                " transfer.after_fence, split.after_install ",
                "split.after_install"
            ));
            assert!(matches("*", "anything"));
            assert!(!matches("split.after_installing", "split.after_install"));
        }
    }
}
