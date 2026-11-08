use anyhow::{anyhow, Result};
use reqwest::{RequestBuilder, Response, StatusCode};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};

#[derive(Clone)]
pub struct RateLimiter {
    interval: Duration,
    max_retries: u32,
    base_delay_ms: u64,
    next_allowed: Arc<Mutex<Instant>>,
}

impl RateLimiter {
    pub fn new(rps: f64, max_retries: u32) -> Self {
        Self::with_base_delay_ms(rps, max_retries, 500)
    }

    pub fn with_base_delay_ms(rps: f64, max_retries: u32, base_delay_ms: u64) -> Self {
        let safe_rps = if rps <= 0.0 { 1.0 } else { rps };
        let interval = Duration::from_secs_f64(1.0 / safe_rps);
        Self {
            interval,
            max_retries,
            base_delay_ms,
            next_allowed: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Acquires a rate-limit slot using token-bucket style interval scheduling.
    pub async fn acquire(&self) {
        let wait_duration = {
            let mut next = self.next_allowed.lock().await;
            let now = Instant::now();
            if *next <= now {
                *next = now + self.interval;
                Duration::ZERO
            } else {
                let wait = *next - now;
                *next += self.interval;
                wait
            }
        };

        if !wait_duration.is_zero() {
            sleep(wait_duration).await;
        }
    }

    /// Sends a request built by `build_req`, pacing with `acquire()` and retrying
    /// with exponential backoff on HTTP 429 or 5xx responses (or transient transport errors).
    pub async fn send_with_retry<F>(&self, build_req: F) -> Result<Response>
    where
        F: Fn() -> RequestBuilder,
    {
        let mut attempt = 0;
        loop {
            self.acquire().await;
            let req = build_req();
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if (status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
                        && attempt < self.max_retries
                    {
                        let delay = Duration::from_millis(self.base_delay_ms * (1_u64 << attempt));
                        attempt += 1;
                        sleep(delay).await;
                        continue;
                    }
                    return Ok(resp);
                }
                Err(err) => {
                    if attempt < self.max_retries {
                        let delay = Duration::from_millis(self.base_delay_ms * (1_u64 << attempt));
                        attempt += 1;
                        sleep(delay).await;
                        continue;
                    }
                    return Err(anyhow!("HTTP request failed after {} retries: {}", attempt, err));
                }
            }
        }
    }
}
