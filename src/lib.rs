#![cfg_attr(not(all(target_family = "wasm", feature = "web")), forbid(unsafe_code))]
#![deny(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

cfg_select! {
    all(target_family = "wasm", feature = "web") => {
        mod web;
        pub use web::*;
    }
    _ => {
        mod native;
        pub use native::*;
    }
}

mod ext;

#[doc(no_inline)]
pub use tokio::{io, join, pin, select, sync, task_local, try_join};

#[doc(no_inline)]
pub use task::spawn;
