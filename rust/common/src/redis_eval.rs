//! Redis Lua command construction helpers.
//!
//! These helpers intentionally only build commands. Service crates still own
//! connection management, error mapping, script bodies, and key ownership.

use std::sync::OnceLock;

/// A process-cached Redis script with a static source body.
///
/// Direct invocations use redis-rs' EVALSHA and NOSCRIPT recovery. Repeated
/// invocations in one pipeline load the script first and then use EVALSHA;
/// singleton pipeline invocations keep using source EVAL. Pipelines are never
/// replayed after an error because earlier commands may already have committed.
pub struct StaticRedisScript {
    body: &'static str,
    script: OnceLock<redis::Script>,
}

#[derive(Clone, Copy)]
pub struct PreparedPipelineScript<'a> {
    script: &'a StaticRedisScript,
    preloaded: bool,
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
        let script = self.cached_script();
        let mut invocation = script.prepare_invoke();
        for key in keys {
            invocation.key(*key);
        }
        for arg in args {
            invocation.arg(*arg);
        }
        invocation
    }

    pub fn prepare_pipeline<'a>(
        &'a self,
        pipe: &mut redis::Pipeline,
        invocation_count: usize,
    ) -> PreparedPipelineScript<'a> {
        let preloaded = invocation_count > 1;
        if preloaded {
            pipe.cmd("SCRIPT").arg("LOAD").arg(self.body).ignore();
        }
        PreparedPipelineScript {
            script: self,
            preloaded,
        }
    }

    fn cached_script(&self) -> &redis::Script {
        self.script.get_or_init(|| redis::Script::new(self.body))
    }
}

impl PreparedPipelineScript<'_> {
    pub fn append(&self, pipe: &mut redis::Pipeline, keys: &[&str], args: &[&str]) {
        if self.preloaded {
            pipe.cmd("EVALSHA")
                .arg(self.script.cached_script().get_hash())
                .arg(keys.len());
            for key in keys {
                pipe.arg(*key);
            }
            for arg in args {
                pipe.arg(*arg);
            }
        } else {
            append_eval_cmd(pipe, self.script.body, keys, args);
        }
    }
}

fn append_eval_cmd(pipe: &mut redis::Pipeline, script: &str, keys: &[&str], args: &[&str]) {
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

    #[test]
    fn pipeline_script_keeps_single_invocations_source_based() {
        static SCRIPT: StaticRedisScript = StaticRedisScript::new("return KEYS[1]");

        let mut actual = redis::pipe();
        let prepared = SCRIPT.prepare_pipeline(&mut actual, 1);
        prepared.append(&mut actual, &["k1"], &["a1"]);

        let mut expected = redis::pipe();
        expected
            .cmd("EVAL")
            .arg("return KEYS[1]")
            .arg(1)
            .arg("k1")
            .arg("a1");
        assert_eq!(actual.get_packed_pipeline(), expected.get_packed_pipeline());
    }

    #[test]
    fn pipeline_script_loads_repeated_invocations_once() {
        static SCRIPT: StaticRedisScript = StaticRedisScript::new("return KEYS[1]");

        let mut actual = redis::pipe();
        let prepared = SCRIPT.prepare_pipeline(&mut actual, 2);
        prepared.append(&mut actual, &["k1"], &["a1"]);
        prepared.append(&mut actual, &["k2"], &["a2"]);

        let mut expected = redis::pipe();
        expected
            .cmd("SCRIPT")
            .arg("LOAD")
            .arg("return KEYS[1]")
            .ignore()
            .cmd("EVALSHA")
            .arg("4a2267357833227dd98abdedb8cf24b15a986445")
            .arg(1)
            .arg("k1")
            .arg("a1")
            .cmd("EVALSHA")
            .arg("4a2267357833227dd98abdedb8cf24b15a986445")
            .arg(1)
            .arg("k2")
            .arg("a2");
        assert_eq!(actual.get_packed_pipeline(), expected.get_packed_pipeline());
    }
}
