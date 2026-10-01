use std::time::Duration;

use jito_protos::auth::{
    auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
    GenerateAuthTokensRequest, Role,
};
use jito_protos::bundle::{bundle_result::Result as BundleResultType, Bundle};
use jito_protos::packet::{Meta, Packet};
use jito_protos::searcher::{
    searcher_service_client::SearcherServiceClient, GetTipAccountsRequest, SendBundleRequest,
    SubscribeBundleResultsRequest,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
    system_instruction,
    transaction::{Transaction, VersionedTransaction},
};
use std::str::FromStr;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

#[tokio::main]
async fn main() {
    // usage: searcher_bot <keypair_path> <block_engine_url> <rpc_url> <tip_lamports> <bundle_count>
    let args: Vec<String> = std::env::args().collect();

    let keypair_path = args.get(1).expect("missing keypair path arg");
    let block_engine_url = args.get(2).expect("missing block engine url arg");
    let rpc_url = args.get(3).expect("missing rpc url arg");
    let tip_lamports: u64 = args
        .get(4)
        .expect("missing tip lamports arg")
        .parse()
        .expect("tip lamports must be a number");
    let bundle_count: u32 = args
        .get(5)
        .expect("missing bundle count arg")
        .parse()
        .expect("bundle count must be a number");

    let keypair = read_keypair_file(keypair_path).expect("failed to read keypair file");
    println!("loaded keypair for pubkey {}", keypair.pubkey());

    let rpc_client = RpcClient::new(rpc_url.to_string());

    // ---- auth with the block engine ----
    let auth_channel = Channel::from_shared(block_engine_url.clone())
        .expect("bad block engine url")
        .tls_config(ClientTlsConfig::new())
        .unwrap()
        .connect()
        .await
        .expect("connect auth channel");

    let mut auth_client = AuthServiceClient::new(auth_channel);

    let challenge_resp = auth_client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Searcher as i32,
            pubkey: keypair.pubkey().to_bytes().to_vec(),
        })
        .await
        .expect("generate auth challenge");

    let challenge = challenge_resp.into_inner().challenge;
    println!("challenge: {}", challenge); // debug

    // sign the challenge and send it back
    let token_resp = auth_client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge: challenge.clone(),
            client_pubkey: keypair.pubkey().to_bytes().to_vec(),
            signed_challenge: keypair.sign_message(challenge.as_bytes()).as_ref().to_vec(),
        })
        .await
        .expect("generate auth tokens");

    let tokens = token_resp.into_inner();
    let access_token = tokens.access_token.unwrap().value;
    // println!("access_token = {}", access_token);
    println!("got access token, len {}", access_token.len());

    // ---- searcher client ----
    let searcher_channel = Channel::from_shared(block_engine_url.clone())
        .expect("bad block engine url")
        .tls_config(ClientTlsConfig::new())
        .unwrap()
        .connect()
        .await
        .expect("connect searcher channel");

    let mut searcher_client =
        SearcherServiceClient::with_interceptor(searcher_channel, move |mut req: Request<()>| {
            req.metadata_mut()
                .insert("authorization", format!("Bearer {}", access_token).parse().unwrap());
            Ok(req)
        });

    let mut bundle_results_stream = searcher_client
        .subscribe_bundle_results(SubscribeBundleResultsRequest {})
        .await
        .expect("subscribe to bundle results")
        .into_inner();

    let tip_accounts_resp = searcher_client
        .get_tip_accounts(GetTipAccountsRequest {})
        .await
        .expect("get tip accounts");
    let tip_account_str = tip_accounts_resp
        .into_inner()
        .accounts
        .into_iter()
        .next()
        .expect("no tip accts??");
    let tip_account = Pubkey::from_str(&tip_account_str).expect("bad tip account pubkey");
    println!("using tip account {}", tip_account);

    let mut sent: u32 = 0;
    let mut landed: u32 = 0;

    while sent < bundle_count {
        let recent_blockhash = match rpc_client.get_latest_blockhash() {
            Ok(bh) => bh,
            Err(e) => {
                println!("get_latest_blockhash failed: {:?}", e);
                continue;
            }
        };

        let tx = Transaction::new_signed_with_payer(
            &[system_instruction::transfer(
                &keypair.pubkey(),
                &tip_account,
                tip_lamports,
            )],
            Some(&keypair.pubkey()),
            &[&keypair],
            recent_blockhash,
        );

        let versioned_tx = VersionedTransaction::from(tx);
        let data = bincode::serialize(&versioned_tx).expect("serialize tx");

        let size = data.len() as u64;

        let bundle = Bundle {
            header: None,
            packets: vec![Packet {
                data,
                meta: Some(Meta {
                    size,
                    ..Meta::default()
                }),
            }],
        };

        match searcher_client
            .send_bundle(SendBundleRequest { bundle: Some(bundle) })
            .await
        {
            Ok(resp) => {
                let uuid = resp.into_inner().uuid;
                println!("sent bundle {}", uuid);
                sent += 1;
            }
            Err(e) => {
                println!("send_bundle error: {:?}", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        }

        match tokio::time::timeout(Duration::from_secs(5), bundle_results_stream.message()).await {
            Ok(Ok(Some(result))) => {
                println!("bundle result: {:?}", result);
                if let Some(BundleResultType::Processed(_)) | Some(BundleResultType::Finalized(_)) = result.result {
                    landed += 1;
                }
            }
            Ok(Ok(None)) => {
                println!("block engine hung up on the results stream, nothing left to read");
            }
            Ok(Err(e)) => {
                println!("couldn't read the next result off the stream: {:?}", e);
            }
            Err(_) => {
                println!("timed out waiting for bundle result");
            }
        }

        let land_rate = landed as f64 / sent as f64 * 100.0;
        println!("{}/{} landed ({:.2}%)", landed, sent, land_rate);

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
