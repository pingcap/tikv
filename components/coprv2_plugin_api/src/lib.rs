// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate contains some necessary types and traits for implementing a custom coprocessor plugin
//! for TiKV.
//!
//! Most notably, if you want to write a custom plugin, your plugin needs to implement the
//! [`CoprocessorPlugin`] trait. The plugin then needs to be compiled to a `cdylib`.
//! In order to make your plugin callable, you need to declare a constructor with the
//! [`declare_plugin`] macro.
//!
//! A plugin can interact with the underlying storage via the [`RawStorage`] trait.
//!
//! # Example
//!
//! ```rust
//! use coprv2_plugin_api::*;
//!
//! #[derive(Default)]
//! struct MyPlugin;
//!
//! impl CoprocessorPlugin for MyPlugin {
//!     fn name(&self) -> &'static str { "my-plugin" }
//!
//!     fn on_raw_coprocessor_request(
//!         &self,
//!         region: &Region,
//!         request: &RawRequest,
//!         storage: &dyn RawStorage,
//!     ) -> Result<RawResponse, Box<dyn std::error::Error>> {
//!         Ok(vec![])
//!     }
//! }
//!
//! declare_plugin!(MyPlugin, MyPlugin::default);
//! ```

mod plugin_api;
mod storage_api;

pub use plugin_api::*;
pub use storage_api::*;
