//! Shared shape of the Workflows tick response.
//!
//! Workflows owns construction and serialization. Scheduler owns tolerant parsing and
//! active-poll policy; neither behavior belongs in this module.

#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkflowTickResponse {
    pub workflow_admitted: usize,
    pub workflow_capacity_blocked: bool,
    pub due_moved: usize,
    pub retention_cleaned: usize,
    pub do_alarm_due_moved: usize,
    pub do_alarm_admitted: usize,
    pub do_alarm_capacity_blocked: bool,
}
