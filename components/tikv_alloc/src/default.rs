use super::error::{ProfError, ProfResult};
use crate::AllocStats;
use std::io;

pub fn dump_stats() -> String {
    String::new()
}
pub fn dump_prof(_path: Option<&str>) {}

pub fn fetch_stats() -> io::Result<Option<AllocStats>> {
    Ok(None)
}

pub fn activate_prof() -> ProfResult<()> {
    Err(ProfError::JemallocNotEnabled)
}

pub fn deactivate_prof() -> ProfResult<()> {
    Err(ProfError::JemallocNotEnabled)
}
