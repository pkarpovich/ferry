//! The name a handler is called by.

use std::fmt;

use serde::{Deserialize, Deserializer};

pub const MAX_NAME_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HandlerName(String);

#[derive(Debug, thiserror::Error)]
#[error("invalid handler name {input:?}: {reason}")]
pub struct NameError {
    input: String,
    reason: &'static str,
}

impl HandlerName {
    /// # Errors
    /// Fails when the input is empty, longer than 64 characters, starts with
    /// anything but a lowercase letter or digit, or has a character outside
    /// `[a-z0-9._-]`.
    pub fn parse(input: &str) -> Result<Self, NameError> {
        let fail = |reason| NameError {
            input: input.to_owned(),
            reason,
        };
        if input.is_empty() {
            return Err(fail("name is empty"));
        }
        if input.len() > MAX_NAME_LEN {
            return Err(fail("name is longer than 64 characters"));
        }
        for (index, ch) in input.chars().enumerate() {
            let alnum = ch.is_ascii_lowercase() || ch.is_ascii_digit();
            let punct = ch == '.' || ch == '_' || ch == '-';
            if !alnum && !punct {
                return Err(fail("only a-z, 0-9, '.', '_' and '-' are allowed"));
            }
            if index == 0 && !alnum {
                return Err(fail("name must start with a letter or digit"));
            }
        }
        Ok(Self(input.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HandlerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for HandlerName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let input = String::deserialize(deserializer)?;
        Self::parse(&input).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_names() {
        for input in ["a", "service-proxy", "x.y_z-1", "0abc", &"a".repeat(64)] {
            let name = HandlerName::parse(input).expect("valid name");
            assert_eq!(name.as_str(), input);
            assert_eq!(name.to_string(), input);
        }
    }

    #[test]
    fn rejects_invalid_names() {
        let too_long = "a".repeat(65);
        for input in [
            "",
            too_long.as_str(),
            "Upper",
            "-lead",
            ".lead",
            "_lead",
            "a/b",
            "sp ace",
        ] {
            let err = HandlerName::parse(input).expect_err("invalid name");
            assert!(err.to_string().contains(&format!("{input:?}")), "{err}");
        }
    }
}
