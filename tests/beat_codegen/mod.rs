#[tokio::test]
async fn test_basic_use() {
    let _beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://localhost:5672//".into()) },
        tasks = [],
        task_routes = []
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_basic_use_with_variable() {
    let connection_string = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://localhost:5672//".into());
    let _app = celery::beat!(
        broker = AMQPBroker { connection_string },
        tasks = [],
        task_routes = []
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_basic_use_with_trailing_comma() {
    let _beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://localhost:5672//".into()) },
        tasks = [],
        task_routes = [],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_with_options() {
    let _beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://localhost:5672//".into()) },
        tasks = [],
        task_routes = [],
        default_queue = "celery"
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_with_options_and_trailing_comma() {
    let _beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://localhost:5672//".into()) },
        tasks = [],
        task_routes = [],
        default_queue = "celery",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_tasks_and_task_routes_with_trailing_comma() {
    let _beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://localhost:5672//".into()) },
        tasks = [,],
        task_routes = [,],
    )
    .await
    .unwrap();
}
