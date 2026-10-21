use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const TYPES: [&str; 3] = ["text.analyze", "report.generate", "benchmark.compute"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Text {
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Report {
    values: Vec<f64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Compute {
    seed: String,
    rounds: u32,
}

pub fn validate(typ: &str, payload: &Value) -> Result<(), String> {
    if !payload.is_object() {
        return Err("payload must be an object".into());
    }
    match typ {
        "text.analyze" => {
            let p: Text = serde_json::from_value(payload.clone()).map_err(|e| e.to_string())?;
            if p.text.is_empty() || p.text.len() > 32768 {
                return Err("text must contain 1..32768 bytes".into());
            }
        }
        "report.generate" => {
            let p: Report = serde_json::from_value(payload.clone()).map_err(|e| e.to_string())?;
            if p.values.is_empty()
                || p.values.len() > 1000
                || p.values.iter().any(|v| !v.is_finite() || v.abs() > 1e12)
            {
                return Err("values must contain 1..1000 finite numbers within +/-1e12".into());
            }
        }
        "benchmark.compute" => {
            let p: Compute = serde_json::from_value(payload.clone()).map_err(|e| e.to_string())?;
            if p.seed.len() > 4096 || !(1..=100000).contains(&p.rounds) {
                return Err("seed <=4096 bytes and rounds 1..100000 required".into());
            }
        }
        _ => return Err("unregistered task type".into()),
    }
    Ok(())
}

// Yield during CPU work so timeout/cancellation can drop this future even on one CPU.
pub async fn execute(typ: &str, payload: &Value) -> Result<Value, String> {
    validate(typ, payload)?;
    match typ {
        "text.analyze" => {
            let p: Text = serde_json::from_value(payload.clone()).map_err(|e| e.to_string())?;
            Ok(
                json!({"words": p.text.split_whitespace().count(), "characters": p.text.chars().count(), "lines": p.text.bytes().filter(|b| *b == b'\n').count()+1}),
            )
        }
        "report.generate" => {
            let p: Report = serde_json::from_value(payload.clone()).map_err(|e| e.to_string())?;
            let sum: f64 = p.values.iter().sum();
            Ok(
                json!({"count":p.values.len(), "sum":sum, "mean":sum/p.values.len() as f64,
                "min":p.values.iter().copied().fold(f64::INFINITY,f64::min), "max":p.values.iter().copied().fold(f64::NEG_INFINITY,f64::max)}),
            )
        }
        "benchmark.compute" => {
            let p: Compute = serde_json::from_value(payload.clone()).map_err(|e| e.to_string())?;
            let mut bytes = p.seed.into_bytes();
            for i in 0..p.rounds {
                if i % 128 == 0 {
                    tokio::task::yield_now().await;
                }
                bytes = Sha256::digest(&bytes).to_vec();
            }
            Ok(json!({"sha256": hex::encode(bytes), "rounds":p.rounds}))
        }
        _ => Err("unregistered task type".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn results_and_validation() {
        assert_eq!(
            execute("text.analyze", &json!({"text":"Zażółć 🌍\nRust"}))
                .await
                .unwrap(),
            json!({"words":3,"characters":13,"lines":2})
        );
        let r = execute("report.generate", &json!({"values":[2,4,8,16]}))
            .await
            .unwrap();
        assert_eq!(r["sum"], 30.0);
        assert_eq!(r["mean"], 7.5);
        assert_eq!(
            execute("benchmark.compute", &json!({"seed":"abc","rounds":1}))
                .await
                .unwrap()["sha256"],
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(validate("text.analyze", &json!({"text":"ok","typo":1})).is_err());
        assert!(validate("report.generate", &json!({"values":[]})).is_err());
        assert!(validate("benchmark.compute", &json!({"seed":"a","rounds":100001})).is_err());
    }
}
