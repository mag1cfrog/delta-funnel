//! Scoped Linux Yama authorization for managed Perfetto tracers.

use std::{fs, io, path::Path};

#[derive(Debug)]
pub(crate) struct PtraceAuthorizationError {
    pub(crate) kind: &'static str,
    pub(crate) message: String,
}

impl PtraceAuthorizationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            kind: "ptrace_permission_failed",
            message: message.into(),
        }
    }
}

pub(crate) fn authorize_tracebox(tracebox_pid: u32) -> Result<(), PtraceAuthorizationError> {
    if tracebox_pid == 0 {
        return Err(PtraceAuthorizationError::new(
            "Perfetto tracebox PID must be positive",
        ));
    }
    if !ptrace_scope_is_relational()? {
        return Ok(());
    }
    // Yama removes this process-wide declaration when the managed tracer exits.
    // Do not clear it blindly: another thread may have replaced the single slot.
    set_ptracer(tracebox_pid.into()).map_err(|_| {
        PtraceAuthorizationError::new(
            "Perfetto tracebox could not be authorized to inspect this process",
        )
    })
}

pub(crate) fn ptrace_scope_is_relational() -> Result<bool, PtraceAuthorizationError> {
    ptrace_scope_is_relational_at(Path::new("/proc/sys/kernel/yama/ptrace_scope"))
}

fn ptrace_scope_is_relational_at(path: &Path) -> Result<bool, PtraceAuthorizationError> {
    let scope = match fs::read_to_string(path) {
        Ok(scope) => scope,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(_) => {
            return Err(PtraceAuthorizationError::new(
                "Linux Yama ptrace mode could not be inspected",
            ));
        }
    };
    match scope.trim() {
        "0" => Ok(false),
        "1" => Ok(true),
        "2" | "3" => Err(PtraceAuthorizationError::new(
            "Linux Yama ptrace mode does not support scoped tracebox authorization",
        )),
        _ => Err(PtraceAuthorizationError::new(
            "Linux Yama ptrace mode was not recognized",
        )),
    }
}

fn set_ptracer(pid: libc::c_ulong) -> io::Result<()> {
    let zero: libc::c_ulong = 0;
    // SAFETY: every variadic argument has C unsigned-long width; PR_SET_PTRACER
    // reads arg2 as a PID, ignores the zeroed remaining arguments, and retains no pointer.
    if unsafe { libc::prctl(libc::PR_SET_PTRACER, pid, zero, zero, zero) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_inspection_rejects_restrictive_unreadable_and_unknown_values() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let scope = directory.path().join("ptrace_scope");
        for (value, relational) in [("0\n", false), ("1\n", true)] {
            fs::write(&scope, value)?;
            assert_eq!(
                ptrace_scope_is_relational_at(&scope).map_err(test_error)?,
                relational,
            );
        }

        for value in ["2\n", "3\n"] {
            fs::write(&scope, value)?;
            let error = ptrace_scope_is_relational_at(&scope)
                .expect_err("a restrictive Yama mode must fail explicitly");
            assert_eq!(error.kind, "ptrace_permission_failed");
        }

        fs::write(&scope, "unexpected\n")?;
        let error = ptrace_scope_is_relational_at(&scope)
            .expect_err("an unknown Yama mode must fail explicitly");
        assert_eq!(error.kind, "ptrace_permission_failed");

        fs::remove_file(&scope)?;
        assert!(
            !ptrace_scope_is_relational_at(&scope).map_err(test_error)?,
            "a missing Yama sysctl means the LSM is unavailable"
        );

        let error = ptrace_scope_is_relational_at(directory.path())
            .expect_err("an unreadable Yama sysctl must fail explicitly");
        assert_eq!(error.kind, "ptrace_permission_failed");
        Ok(())
    }

    #[test]
    fn zero_is_never_a_valid_managed_tracebox_pid() {
        let error =
            authorize_tracebox(0).expect_err("PID zero must not clear the Yama declaration");
        assert_eq!(error.kind, "ptrace_permission_failed");
    }

    fn test_error(error: PtraceAuthorizationError) -> io::Error {
        io::Error::other(format!("unexpected Yama authorization error: {error:?}"))
    }
}
