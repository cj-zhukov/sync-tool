use env_logger::Env;

pub fn init_logger() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .write_style(env_logger::WriteStyle::Always)
        .format_timestamp_secs()
        .init();
}
