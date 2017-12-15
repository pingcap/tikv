
pub const HIGH_PRI: i32 = -1;

#[cfg(target_os = "linux")]
pub mod pri {
    use libc;
    use std::io::{Error, ErrorKind};

    pub fn set_priority(pri: i32) -> Result<(), Error> {
        unsafe {
            let pid = libc::getpid();
            if libc::setpriority(libc::PRIO_PROCESS as u32, pid as u32, pri) != 0 {
                let e = Error::last_os_error();
                return Err(e);
            }
            Ok(())
        }
    }

    pub fn get_priority() -> Result<i32, Error> {
        unsafe {
            let pid = libc::getpid();
            let ret = libc::getpriority(libc::PRIO_PROCESS as u32, pid as u32);
            if ret == -1 {
                let e = Error::last_os_error();
                if e.kind() != ErrorKind::Other {
                    return Err(e);
                }
            }
            Ok(ret)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::ErrorKind;

        #[test]
        #[cfg(target_os = "linux")]
        fn test_set_priority() {
            // priority is a value in range -20 to 19, the default priority
            // is 0, lower priorities cause more favorable scheduling.
            assert_eq!(get_priority(), Ok(0));
            assert!(set_proitiry(10).is_ok());

            assert_eq!(get_priority(), Ok(10));

            // only users which have `SYS_NICE_CAP` capability can priority.
            let ret = set_proitiry(-10);
            if let ret = Err(e) {
                assert_eq!(e.kind(), ErrorKind::PermissionDenied);
            } else {
                assert_eq!(get_priority(), Ok(-10));
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub mod pri {
    use std::io::Error;

    pub fn set_priority(_: i32) -> Result<(), Error> {
        Ok(())
    }

    pub fn get_priority() -> Result<i32, Error> {
        Ok(0)
    }
}
