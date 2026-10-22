extern crate hyper;
extern crate log;
extern crate pusher;
extern crate tokio;
#[cfg(test)]
#[macro_use]
extern crate yup_hyper_mock;

use hyper::StatusCode;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use pusher::{BatchEvent, Error, PusherBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mock_connector!(BadRequest {
    "http://127.0.0.1" =>       "HTTP/1.1 400 Bad Request\r\n\
                                 Server: mock1\r\n\
                                 \r\n\
                                 Cannot retrieve the user count unless the channel is a presence channel"
});

mock_connector!(TriggerEBTest {
    "http://127.0.0.1" =>       "HTTP/1.1 200 OK\r\n\
                                 Server: mock1\r\n\
                                 \r\n\
                                 {\"event_ids\":{\"test_channel\":\"eudhq1809scss2\"}}"
});

mock_connector!(ChannelsRequest {
    "http://127.0.0.1" =>       "HTTP/1.1 200 OK\r\n\
                                 Server: mock1\r\n\
                                 \r\n\
                                 {\"channels\":{\"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-5cbTiUiPNGI\":{\"user_count\":1},\"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-PbZ5E1pP8uF\":{\"user_count\":1},\"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-oz6iqpSxMwG\":{\"user_count\":1}}}"
});

mock_connector!(ChannelRequest {
    "http://127.0.0.1" =>       "HTTP/1.1 200 OK\r\n\
                                 Server: mock1\r\n\
                                 \r\n\
                                 {\"user_count\":1,\"occupied\":true,\"subscription_count\":1}"
});

mock_connector!(ChannelUsersRequest {
    "http://127.0.0.1" =>       "HTTP/1.1 200 OK\r\n\
                                 Server: mock1\r\n\
                                 \r\n\
                                 {\"users\":[{\"id\":\"red\"},{\"id\":\"blue\"}]}"
});

#[tokio::test]
async fn test_error_response_handler() {
    let client = Client::builder(TokioExecutor::new()).build(BadRequest::default());
    let pusher = PusherBuilder::new_with_client(client, "1", "2", "3")
        .host("127.0.0.1")
        .finalize();
    let query_params = vec![(
        "info".to_string(),
        "user_count,subscription_count".to_string(),
    )];
    let res = pusher
        .channel_with_options("this_is_not_a_presence_channel", query_params)
        .await;
    assert!(matches!(
        res,
        Err(Error::Response(StatusCode::BAD_REQUEST, _))
    ));
}

#[tokio::test]
async fn test_eb_trigger() {
    let client = Client::builder(TokioExecutor::new()).build(TriggerEBTest::default());
    let pusher = PusherBuilder::new_with_client(client, "1", "2", "3")
        .host("127.0.0.1")
        .finalize();
    let res = pusher.trigger("woot", "yolo", "huh").await;
    let events = res.unwrap();
    println!("{:?}", events);
    let event_id = events.event_ids.unwrap();

    assert_eq!(event_id.get("test_channel").unwrap(), "eudhq1809scss2")
}

#[tokio::test]
async fn test_get_channels() {
    let client = Client::builder(TokioExecutor::new()).build(ChannelsRequest::default());
    let pusher = PusherBuilder::new_with_client(client, "1", "2", "3")
        .host("127.0.0.1")
        .finalize();
    let res = pusher.channels().await;
    let channels = res.unwrap();
    let item = channels
        .channels
        .get("presence-session-d41a439c438a100756f5-4bf35003e819bb138249-5cbTiUiPNGI")
        .unwrap();
    assert_eq!(item.user_count.unwrap(), 1)
}

#[tokio::test]
async fn test_get_channel() {
    let client = Client::builder(TokioExecutor::new()).build(ChannelRequest::default());
    let pusher = PusherBuilder::new_with_client(client, "1", "2", "3")
        .host("127.0.0.1")
        .finalize();
    let res = pusher.channel("presence-for-all").await;
    let channel = res.unwrap();
    let user_count = channel.user_count.unwrap();
    let occupied = channel.occupied.unwrap();
    let subscription_count = channel.subscription_count.unwrap();
    assert_eq!(user_count, 1);
    assert_eq!(occupied, true);
    assert_eq!(subscription_count, 1)
}

#[tokio::test]
async fn test_get_channel_users() {
    let client = Client::builder(TokioExecutor::new()).build(ChannelUsersRequest::default());
    let pusher = PusherBuilder::new_with_client(client, "1", "2", "3")
        .host("127.0.0.1")
        .finalize();
    let res = pusher.channel_users("presence-yolo").await;
    let users = res.unwrap().users;
    let user_one = &users[0];
    let user_two = &users[1];
    assert_eq!(user_one.id, "red");
    assert_eq!(user_two.id, "blue")
}

/// Answers one request with `{}` and returns its request line and body.
async fn capture_one_request(listener: tokio::net::TcpListener) -> (String, String) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut buffer = [0; 4096];
    let (head_end, content_length) = loop {
        let read = socket.read(&mut buffer).await.unwrap();
        request.extend_from_slice(&buffer[..read]);
        let text = String::from_utf8_lossy(&request);
        if let Some(head_end) = text.find("\r\n\r\n") {
            let content_length = text[..head_end]
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(|n| n.parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            break (head_end + 4, content_length);
        }
    };
    while request.len() < head_end + content_length {
        let read = socket.read(&mut buffer).await.unwrap();
        request.extend_from_slice(&buffer[..read]);
    }
    socket
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
        .await
        .unwrap();
    let text = String::from_utf8(request).unwrap();
    let request_line = text.lines().next().unwrap().to_string();
    (request_line, text[head_end..].to_string())
}

#[tokio::test]
async fn test_batch_trigger() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let host = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(capture_one_request(listener));
    let pusher = PusherBuilder::new("1", "2", "3").host(&host).finalize();

    let events = [
        BatchEvent {
            channel: "first_channel",
            event: "first",
            payload: 1,
        },
        BatchEvent {
            channel: "second_channel",
            event: "second",
            payload: 2,
        },
    ];
    pusher.trigger_batch(&events).await.unwrap();

    let (request_line, body) = server.await.unwrap();
    assert!(request_line.starts_with("POST /apps/1/batch_events?"));
    assert_eq!(
        body,
        r#"{"batch":[{"name":"first","channel":"first_channel","data":"1"},{"name":"second","channel":"second_channel","data":"2"}]}"#
    );
}
