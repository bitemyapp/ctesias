use aws_sdk_s3::{Client, Error};
use hifitime::Epoch;
use ring::digest::Context;
use ring::digest::Digest;
use std::io::Read;

fn sha512_digest<R: Read>(mut reader: R) -> Result<Digest, std::io::Error> {
    let mut context = Context::new(&ring::digest::SHA512);
    let mut buffer = [0; 1024];

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.update(&buffer[..count]);
    }

    Ok(context.finish())
}

pub async fn simple_download_file(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &std::path::Path,
) -> Result<(), Error> {
    let mut file = tokio::fs::File::create(path).await.unwrap();
    let mut stream = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .body
        .into_async_read();
    tokio::io::copy(&mut stream, &mut file).await.unwrap();
    Ok(())
}

pub async fn simple_ranged_download_file(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &std::path::Path,
) -> Result<(), Error> {
    let mut file = tokio::fs::File::create(path).await.unwrap();
    let mut stream = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .body
        .into_async_read();
    tokio::io::copy(&mut stream, &mut file).await.unwrap();
    Ok(())
}

async fn create_default_client() -> Client {
    let shared_config = aws_config::load_from_env().await;
    Client::new(&shared_config)
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use super::*;

    const kjv: &str = "s3://ctesias/test_data/kjv.txt";
    const ctesias_bucket: &str = "ctesias";
    const kjv_key: &str = "test_data/kjv.txt";
    const kjv_md5: &str = "e4a5ec9444a0b7cd6d9c16eb228bd804";
    const kjv_sha256: &str = "fa72f9a3666bd0259454a89d7debe42a49fc0c168c60c02073e2b00fc8ce52f4";
    const kjv_sha512: &str = "755c4e1631d2bdc61f23fdaa71ebdab8c1da2050c6de62d038a81a39e99844c25fa26c03625f79c4c9fe4e05a3b1aa2b28c5829780db902a8f182840c20c82d9";
    const seven_key: &str = "test_data/seven.mp4";
    const seven_md5: &str = "282326fafe14d6cd851ca9ad7612c0ab";
    const seven_sha256: &str = "ed3453f94aec2ce09a9b1433e064c684f057b45b451de749d244c31f35610706";
    const seven_sha512: &str = "207b5f5327df73aa4d86f97f34ae8ed14508970cb22306ccf099bc45baf9effcf12b2f39ce5cc6bc24c0adba566c6e4d7791b7e5ba2ed38ad89e4b843761bd4f";

    #[tokio::test]
    async fn test_list_buckets() -> Result<(), Error> {
        let client = create_default_client().await;
        let resp = client.list_buckets().send().await?;
        // println!("Current buckets: {:#?}", resp);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_object() -> Result<(), Error> {
        let client = create_default_client().await;
        let resp = client.get_object().bucket(ctesias_bucket).key(kjv_key).send().await?;
        let body = resp.body.collect().await.expect("Failed to collect bytes");
        let digest = sha512_digest(body.reader()).unwrap();
        let digest_str = hex::encode(digest.as_ref());
        assert_eq!(digest_str, kjv_sha512);
        // let sha256 = resp.checksum_sha256().unwrap();
        // assert_eq!(sha256, kjv_sha256);
        println!("Resp: {:#?}", digest);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_kjv() -> Result<(), Error> {
        // 1-5 seconds on a 400-500 Mbps connection typically
        let client = create_default_client().await;
        let now = Epoch::now().unwrap();
        simple_download_file(&client, ctesias_bucket, kjv_key, std::path::Path::new("./tmp/kjv.txt")).await?;
        let later = Epoch::now().unwrap();
        let delta_time = later - now;
        println!("Delta time: {}", delta_time);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_seven() -> Result<(), Error> {
        // ~50 MiB/s -> 419.4304 megabit / second
        // 40 seconds on a 400-500 Mbps connection typically
        let client = create_default_client().await;
        let now = Epoch::now().unwrap();
        simple_download_file(&client, ctesias_bucket, seven_key, std::path::Path::new("./tmp/seven.mp4")).await?;
        let later = Epoch::now().unwrap();
        let delta_time = later - now;
        println!("Delta time: {}", delta_time);
        Ok(())
    }
}
