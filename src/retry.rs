use std::time::Duration;
use uuid::Uuid;
pub(crate) fn random_jitter_factor() -> f64 {
    let bytes = Uuid::new_v4().into_bytes();
    let sample = f64::from(u16::from_be_bytes([bytes[0], bytes[1]])) / f64::from(u16::MAX);
    0.5 + sample
}

pub(crate) fn jittered_backoff(base: Duration, exponent: u32, jitter: f64) -> Duration {
    let multiplier = 2_u64.saturating_pow(exponent.min(20));
    base.saturating_mul(multiplier as u32)
        .mul_f64(jitter.clamp(0.5, 1.5))
}
