//! Backend implementations of `SandboxRuntime`.

pub mod local;

#[cfg(feature = "backend-docker")]
pub mod docker;

#[cfg(feature = "backend-sprites")]
pub mod sprites;

pub use local::LocalProcessRuntime;

#[cfg(feature = "backend-docker")]
pub use docker::DockerRuntime;

#[cfg(feature = "backend-sprites")]
pub use sprites::SpritesRuntime;
