//! Redis Lua command construction helpers.
//!
//! These helpers intentionally only build commands. Service crates still own
//! connection management, error mapping, script bodies, and key ownership.

use std::sync::OnceLock;

/// A process-cached Redis script with a static source body.
///
/// Direct invocations use redis-rs' EVALSHA and NOSCRIPT recovery. Pipeline
/// scripts keep using source constants with [`append_eval_cmd`] because
/// replaying a partially executed pipeline is not safe.
pub struct StaticRedisScript {
    body: &'static str,
    script: OnceLock<redis::Script>,
}

impl StaticRedisScript {
    pub const fn new(body: &'static str) -> Self {
        Self {
            body,
            script: OnceLock::new(),
        }
    }

    pub fn prepare_invoke<'a>(
        &'a self,
        keys: &[&str],
        args: &[&str],
    ) -> redis::ScriptInvocation<'a> {
        let script = self.script.get_or_init(|| redis::Script::new(self.body));
        let mut invocation = script.prepare_invoke();
        for key in keys {
            invocation.key(*key);
        }
        for arg in args {
            invocation.arg(*arg);
        }
        invocation
    }
}

pub fn append_eval_cmd(pipe: &mut redis::Pipeline, script: &str, keys: &[&str], args: &[&str]) {
    pipe.cmd("EVAL").arg(script).arg(keys.len());
    for key in keys {
        pipe.arg(*key);
    }
    for arg in args {
        pipe.arg(*arg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_eval_cmd_derives_numkeys_from_key_slice() {
        let mut actual = redis::pipe();
        append_eval_cmd(&mut actual, "return KEYS[1]", &["k1", "k2"], &["a1"]);

        let mut expected = redis::pipe();
        expected
            .cmd("EVAL")
            .arg("return KEYS[1]")
            .arg(2)
            .arg("k1")
            .arg("k2")
            .arg("a1");
        assert_eq!(actual.get_packed_pipeline(), expected.get_packed_pipeline());
    }

    #[test]
    fn static_script_reuses_the_cached_redis_script() {
        static SCRIPT: StaticRedisScript = StaticRedisScript::new("return KEYS[1]");

        let first = SCRIPT
            .script
            .get_or_init(|| redis::Script::new(SCRIPT.body));
        let second = SCRIPT
            .script
            .get_or_init(|| redis::Script::new(SCRIPT.body));
        assert!(std::ptr::eq(first, second));
    }
}
