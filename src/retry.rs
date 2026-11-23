use std::time::Duration;
use uuid::Uuid;

pub(crate) fn random_jitter_factor() -> f64 {
    let bytes = Uuid::new_v4().into_bytes();
    let sample = f64::from(u16::from_be_bytes([bytes[0], bytes[1]])) / f64::from(u16::MAX);
    0.5 + sample
}

pub(crate) fn jittered_backoff(
    base: Duration,
    exponent: u32,
    jitter: f64,
    cap: Duration,
) -> Duration {
    let multiplier = 2_u64.saturating_pow(exponent.min(20));
    let nanos = base.saturating_mul(multiplier as u32).min(cap).as_nanos();
    let scale = (jitter.clamp(0.5, 1.5) * 65536.0).round() as u128;
    let scaled = (nanos * scale / 65536).min(cap.as_nanos());
    Duration::new(
        (scaled / 1_000_000_000) as u64,
        (scaled % 1_000_000_000) as u32,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extreme_backoff_retains_jitter_at_cap() {
        for cap in [Duration::MAX, Duration::from_secs(120)] {
            assert_eq!(jittered_backoff(Duration::MAX, u32::MAX, 0.5, cap), cap / 2);
            assert_eq!(jittered_backoff(Duration::MAX, u32::MAX, 1.5, cap), cap);
        }
    }
}
