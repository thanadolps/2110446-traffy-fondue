use std::sync::atomic::{self, AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, io::Write};

use anyhow::{Context, Ok, Result};
use futures::stream::FuturesUnordered;
use futures::stream::{iter, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio_utils::RateLimiter;

const BASE_URL: &str = "https://publicapi.traffy.in.th/share/teamchadchart/search";
const REQUEST_LIMIT: u32 = 1000;

#[derive(Serialize, Deserialize, Debug)]
struct TraffyResponse {
    status: bool,
    total: Option<u32>,
    results: Vec<serde_json::Value>,
}

async fn download(client: &Client, offset: u32, limit: Option<u32>) -> Result<TraffyResponse> {
    let limit = limit.unwrap_or(REQUEST_LIMIT);
    let response = client
        .get(BASE_URL)
        .query(&[("offset", offset), ("limit", limit)])
        .send()
        .await?
        .json::<TraffyResponse>()
        .await?;
    assert_eq!(response.status, true);
    Ok(response)
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = reqwest::Client::new();

    let output_file = fs::File::create("../data/traffy_raw_data.json")?;
    let output_writer = std::io::BufWriter::new(output_file);
    let output_writer = Arc::new(Mutex::new(output_writer));

    // Sample download to get total number of records
    println!("Downloading sample data...");
    let response = download(&client, 0, Some(1)).await?;
    assert_eq!(response.results.len(), 1);
    let total_record = response
        .total
        .context("Fail to read `total` field from first sample request")?;
    println!("Total record: {}", total_record);

    // Start downloading
    output_writer.lock().unwrap().write(b"[")?;

    // limiter
    let ratelimiter = RateLimiter::new(Duration::from_secs_f32(3.0));
    let concurrent_limiter = tokio::sync::Semaphore::new(5);

    let is_first_object = AtomicBool::new(true);

    // setup tasks
    let mut tasks = (0..total_record)
        .step_by(REQUEST_LIMIT as usize)
        .map(|offset| {
            let client = &client;
            let output_writer = output_writer.clone();
            let cl = &concurrent_limiter;
            let is_first_object = &is_first_object;

            ratelimiter.throttle(move || async move {
                let permit = cl.acquire().await.unwrap();

                let mut backoff_delay = 5.0;

                let response = loop {
                    println!(
                        "Downloading record {}-{} (total record={})...",
                        offset,
                        offset + REQUEST_LIMIT - 1,
                        total_record
                    );
                    let response = download(&client, offset, Some(REQUEST_LIMIT)).await;

                    match response {
                        Err(e) => {
                            println!("Error: {}", e);
                            println!("Retrying in {} seconds...", backoff_delay);
                            tokio::time::sleep(Duration::from_secs_f32(backoff_delay)).await;

                            // exponential backoff
                            backoff_delay = (backoff_delay * 1.5).min(180.0);
                        }
                        core::result::Result::Ok(res) => {
                            break res;
                        }
                    }
                };

                println!(
                    "Downloaded record {}-{} (total record={})...",
                    offset,
                    offset + REQUEST_LIMIT - 1,
                    total_record
                );
                println!("Recived {} records", response.results.len());
                println!("Writing to file...");
                let mut wr = &mut *output_writer.lock().unwrap();
                for result in &response.results {
                    if !is_first_object.swap(false, atomic::Ordering::SeqCst) {
                        wr.write(b",").unwrap();
                    }
                    serde_json::to_writer(&mut wr, &result).unwrap();
                }

                drop(permit);
            })
        })
        .collect::<FuturesUnordered<_>>();

    while let Some(_) = tasks.next().await {}

    output_writer.lock().unwrap().write(b"]")?;

    Ok(())
}
