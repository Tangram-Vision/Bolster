//! A module to parse an object space TOML file for use in the Tangram Vision calibration system.

use std::{fs::read_to_string, path::Path};

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// A type representing the possible object-space configurations.
///
/// Configurations comprise a detector-descriptor pairing for each component type within the
/// system. This means that cameras will have a distinct detector / descriptor pairing from e.g.
/// LiDAR components.
///
/// At the present time, only cameras are currently supported.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ObjectSpaceConfig {
    /// Configuration for camera components.
    camera: DetectorDescriptor,
}

/// A type representing the detector-descriptor pairing for a camera.
///
/// Not every variant of detector and descriptor is guaranteed to be semantically valid when paired
/// together.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub struct DetectorDescriptor {
    /// The detector to use on observations from the parent component type.
    detector: Detector,

    /// The descriptor to define the object-space we are observing in observations with the
    /// detector.
    descriptor: Descriptor,
}

/// A type describing the possible detectors that can be used on component observations, and their
/// parameters.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum Detector {
    /// Detector for a checkerboard within a camera image.
    ///
    /// Valid descriptors are:
    ///
    /// - `"detector_defined"`
    Checkerboard {
        /// Number of checker squares horizontally on the board.
        width: i64,
        /// Number of checker squares vertically on the board.
        height: i64,
        /// Size of one edge of a checker square, in metres.
        edge_length: f64,
        /// The variances (X/Y/Z) of object-space points, in metres^2.
        variances: [f64; 3],
    },

    /// Detector for a ChArUco board within a camera image.
    ///
    /// Valid descriptors are:
    ///
    /// - `"detector_defined"`
    Charuco {
        /// Number of checker squares horizontally on the board.
        width: i64,
        /// Number of checker squares vertically on the board.
        height: i64,
        /// Size of one edge of a checker square, in metres.
        edge_length: f64,
        /// Size of one edge of the ArUco markers in the board.
        ///
        /// Should be smaller than `edge_length`.
        marker_length: f64,
        /// The variances (X/Y/Z) of object-space points, in metres^2.
        variances: [f64; 3],
    },
}

/// A type describing the possible descriptors for the object-space detected in an image.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum Descriptor {
    /// The descriptor is to be defined in terms of the detector and its parameters.
    DetectorDefined,
}

/// A function to read in the object space config from a TOML file at the given path.
pub fn read_object_space_config<P>(toml_path: P) -> Result<ObjectSpaceConfig>
where
    P: AsRef<Path>,
{
    Ok(toml::from_str(&read_to_string(toml_path)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_checkerboard_is_ok() {
        read_object_space_config("fixtures/checkerboard_detector.toml").unwrap();
    }

    #[test]
    fn valid_charuco_is_ok() {
        read_object_space_config("fixtures/charuco_detector.toml").unwrap();
    }

    #[test]
    fn invalid_toml_does_not_parse() {
        read_object_space_config("Cargo.toml").unwrap_err();
    }

    #[test]
    fn file_that_does_not_exist_is_err() {
        read_object_space_config("fixtures/i-do-not-exist.png").unwrap_err();
    }
}
