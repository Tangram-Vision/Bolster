// Copyright (c) 2021 Tangram Robotics Inc. - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// ----------------------------

#![feature(test)]

extern crate core;
extern crate test;

use core::hazard;

use test::Bencher;

#[bench]
fn bench_hazard(b: &mut Bencher) {
    b.iter(|| {
        hazard::generate_hazard();
    })
}
