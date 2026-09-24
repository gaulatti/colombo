use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    sync::Mutex,
    time::{Duration, Instant},
};

const WINDOW: Duration = Duration::from_secs(60);
const MAX_ENTRIES: usize = 8192;

#[derive(Default)]
pub struct LoginThrottle {
    entries: Mutex<HashMap<(i64, IpAddr), VecDeque<Instant>>>,
}

impl LoginThrottle {
    pub fn retry_after(&self, tenant_id: i64, ip: IpAddr, limit: Option<i32>) -> Option<u64> {
        let limit = limit? as usize;
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap();
        Self::prune(&mut entries, now);
        let failures = entries.get(&(tenant_id, ip))?;
        if failures.len() >= limit {
            Some(
                WINDOW
                    .saturating_sub(now.duration_since(*failures.front().unwrap()))
                    .as_secs()
                    .max(1),
            )
        } else {
            None
        }
    }

    pub fn denied(&self, tenant_id: i64, ip: IpAddr, limit: Option<i32>) {
        if limit.is_none() {
            return;
        }
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap();
        Self::prune(&mut entries, now);
        let key = (tenant_id, ip);
        if !entries.contains_key(&key)
            && entries.len() >= MAX_ENTRIES
            && let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, failures)| failures.back())
                .map(|(key, _)| *key)
        {
            entries.remove(&oldest);
        }
        let failures = entries.entry(key).or_default();
        failures.push_back(now);
        while failures.len() > limit.unwrap() as usize {
            failures.pop_front();
        }
    }

    pub fn success(&self, tenant_id: i64, ip: IpAddr) {
        self.entries.lock().unwrap().remove(&(tenant_id, ip));
    }

    fn prune(entries: &mut HashMap<(i64, IpAddr), VecDeque<Instant>>, now: Instant) {
        entries.retain(|_, failures| {
            while failures
                .front()
                .is_some_and(|at| now.duration_since(*at) >= WINDOW)
            {
                failures.pop_front();
            }
            !failures.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn budget_is_per_ip_and_tenant_and_success_clears_one_key() {
        let throttle = LoginThrottle::default();
        let a = IpAddr::from([192, 0, 2, 1]);
        let b = IpAddr::from([192, 0, 2, 2]);
        for _ in 0..10 {
            throttle.denied(1, a, Some(10));
        }
        assert!(throttle.retry_after(1, a, Some(10)).is_some());
        assert_eq!(throttle.retry_after(1, b, Some(10)), None);
        assert_eq!(throttle.retry_after(2, a, Some(10)), None);
        assert_eq!(throttle.retry_after(1, a, None), None);
        for _ in 0..10 {
            throttle.denied(1, b, Some(10));
        }
        throttle.success(1, a);
        assert_eq!(throttle.retry_after(1, a, Some(10)), None);
        assert!(throttle.retry_after(1, b, Some(10)).is_some());
    }

    #[test]
    fn expired_failures_leave_window_and_flood_is_bounded() {
        let throttle = LoginThrottle::default();
        let a = IpAddr::from([192, 0, 2, 1]);
        throttle
            .entries
            .lock()
            .unwrap()
            .insert((1, a), VecDeque::from([Instant::now() - WINDOW]));
        assert_eq!(throttle.retry_after(1, a, Some(1)), None);
        for id in 0..MAX_ENTRIES + 100 {
            throttle.denied(id as i64, a, Some(1));
        }
        assert_eq!(throttle.entries.lock().unwrap().len(), MAX_ENTRIES);
    }
}
