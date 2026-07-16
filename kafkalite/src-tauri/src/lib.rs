mod commands;
mod error;
mod kafka;
mod security;
mod state;
mod storage;

use tauri::Manager;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::state::AppState;
use crate::storage::Database;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    init_tracing();

    // Keep a process-wide Tokio runtime for rdkafka async consumers/producers.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");
    tauri::async_runtime::set(runtime.handle().clone());
    Box::leak(Box::new(runtime));

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let data_dir = app.path().app_data_dir().map_err(|err| err.to_string())?;
            let db_path = data_dir.join("kafkalite.db");
            let db = Database::open(db_path).map_err(|err| err.to_string())?;
            app.manage(AppState::new(db));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::list_connections,
            commands::save_connection,
            commands::delete_connection,
            commands::test_connection,
            commands::connect_cluster,
            commands::disconnect_cluster,
            commands::get_active_connection,
            commands::list_topics,
            commands::describe_topic,
            commands::create_topic,
            commands::delete_topic,
            commands::delete_records_before,
            commands::start_consume,
            commands::stop_consume,
            commands::produce_tombstone,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

fn init_tracing() {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kafkalite=info,tauri=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();
}
