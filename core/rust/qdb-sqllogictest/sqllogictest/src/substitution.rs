use std::sync::{Arc, OnceLock};

use subst::Env;
use tempfile::{tempdir, TempDir};

/// Substitute environment variables and special variables like `__TEST_DIR__` in SQL.
#[derive(Default, Clone)]
pub(crate) struct Substitution {
    /// The temporary directory for `__TEST_DIR__`.
    /// Lazily initialized and cleaned up when dropped.
    test_dir: Arc<OnceLock<TempDir>>,
}

#[derive(thiserror::Error, Debug)]
#[error("substitution failed: {0}")]
pub(crate) struct SubstError(subst::Error);

impl Substitution {
    pub fn substitute(&self, input: &str, subst_env_vars: bool) -> Result<String, SubstError> {
        if !subst_env_vars {
            Ok(input
                .replace("$__TEST_DIR__", &self.test_dir())
                .replace("$__NOW__", &self.now()))
        } else {
            subst::substitute(input, self).map_err(SubstError)
        }
    }

    fn test_dir(&self) -> String {
        let test_dir = self
            .test_dir
            .get_or_init(|| tempdir().expect("failed to create testdir"));
        test_dir.path().to_string_lossy().into_owned()
    }

    fn now(&self) -> String {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("failed to get current time")
            .as_nanos()
            .to_string()
    }
}

impl<'a> subst::VariableMap<'a> for Substitution {
    type Value = String;

    fn get(&'a self, key: &str) -> Option<Self::Value> {
        match key {
            "__TEST_DIR__" => self.test_dir().into(),
            "__NOW__" => self.now().into(),
            key => Env.get(key),
        }
    }
}
