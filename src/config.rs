use anyhow::{bail, Context, Result};
use std::{env, time::Duration};

#[derive(Clone, Debug)]
pub struct AccountConfig {
    pub label: String,
    pub token: String,
}

#[derive(Clone, Debug)]
pub struct ClientProfile {
    pub os: String,
    pub os_version: String,
    pub os_arch: String,
    pub app_arch: String,
    pub system_locale: String,
    pub browser: String,
    pub browser_version: String,
    pub chrome_version: String,
    pub browser_user_agent: String,
    pub client_version: String,
    pub client_build_number: u64,
    pub native_build_number: u64,
    pub release_channel: String,
    pub distro: String,
    pub window_manager: String,
    pub display_server: String,
    pub runtime_environment: String,
    pub device: String,
    pub heartbeat_jitter_percent: u32,
    pub jitter_stddev_percent: u32,
    pub presence_status: String,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub accounts: Vec<AccountConfig>,
    pub guild_id: String,
    pub channel_id: String,
    pub port: u16,
    pub self_mute: bool,
    pub self_deaf: bool,
    pub startup_grace: Duration,
    pub health_fail_after: Duration,
    pub supervisor_backoff_max: Duration,
    pub memory_warn_mb: f64,
    pub memory_restart_mb: f64,
    pub memory_restart_samples: u32,
    pub client_profile: ClientProfile,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let raw = env::var("DISCORD_TOKENS").context("DISCORD_TOKENS is required")?;
        let tokens: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        if tokens.is_empty() {
            bail!("DISCORD_TOKENS contains no tokens");
        }
        let max_accounts = env_u64("MAX_ACCOUNTS", 2)? as usize;
        if tokens.len() > max_accounts {
            bail!(
                "configured {} accounts but MAX_ACCOUNTS={max_accounts}",
                tokens.len()
            );
        }

        let guild_id = env_required_id("TARGET_GUILD_ID")?;
        let channel_id = env_required_id("TARGET_CHANNEL_ID")?;

        let memory_warn_mb = env_f64("MEMORY_WARN_MB", 300.0)?;
        let memory_restart_mb = env_f64("MEMORY_RESTART_MB", 420.0)?;
        if !memory_warn_mb.is_finite() || memory_warn_mb <= 0.0 {
            bail!("MEMORY_WARN_MB must be finite and > 0");
        }
        if !memory_restart_mb.is_finite() || memory_restart_mb <= 0.0 {
            bail!("MEMORY_RESTART_MB must be finite and > 0");
        }
        if memory_restart_mb <= memory_warn_mb {
            bail!("MEMORY_RESTART_MB must be greater than MEMORY_WARN_MB");
        }

        let memory_restart_samples: u32 = env_u64("MEMORY_RESTART_SAMPLES", 3)?
            .try_into()
            .context("MEMORY_RESTART_SAMPLES must fit into u32")?;
        if memory_restart_samples == 0 {
            bail!("MEMORY_RESTART_SAMPLES must be > 0");
        }

        let accounts = tokens
            .into_iter()
            .enumerate()
            .map(|(idx, token)| AccountConfig {
                label: format!("node-{}", idx + 1),
                token,
            })
            .collect();

        let heartbeat_jitter_percent: u32 = env_u64("HEARTBEAT_JITTER_PERCENT", 10)?
            .try_into()
            .context("HEARTBEAT_JITTER_PERCENT must fit into u32")?;
        let jitter_stddev_percent: u32 = env_u64("JITTER_STDDEV_PERCENT", 3)?
            .try_into()
            .context("JITTER_STDDEV_PERCENT must fit into u32")?;
        if heartbeat_jitter_percent == 0 || heartbeat_jitter_percent > 100 {
            bail!("HEARTBEAT_JITTER_PERCENT must be between 1 and 100");
        }
        if jitter_stddev_percent == 0 || jitter_stddev_percent > heartbeat_jitter_percent {
            bail!("JITTER_STDDEV_PERCENT must be between 1 and HEARTBEAT_JITTER_PERCENT");
        }

        let presence_status =
            env::var("PRESENCE_STATUS").unwrap_or_else(|_| "invisible".to_string());
        if !matches!(
            presence_status.as_str(),
            "online" | "idle" | "dnd" | "invisible"
        ) {
            bail!("PRESENCE_STATUS must be one of: online, idle, dnd, invisible");
        }

        let os = env::var("CLIENT_OS").unwrap_or_else(|_| "Linux".to_string());
        if !matches!(os.as_str(), "Linux" | "Windows" | "macOS") {
            bail!("CLIENT_OS must be one of: Linux, Windows, macOS");
        }

        let os_arch = env::var("CLIENT_OS_ARCH").unwrap_or_else(|_| "x86_64".to_string());
        let app_arch = env::var("CLIENT_APP_ARCH").unwrap_or_else(|_| "x86_64".to_string());
        if !matches!(os_arch.as_str(), "x86_64" | "aarch64") {
            bail!("CLIENT_OS_ARCH must be one of: x86_64, aarch64");
        }
        if !matches!(app_arch.as_str(), "x86_64" | "aarch64") {
            bail!("CLIENT_APP_ARCH must be one of: x86_64, aarch64");
        }
        if os_arch != app_arch {
            bail!("CLIENT_OS_ARCH and CLIENT_APP_ARCH must be the same");
        }

        let release_channel = env::var("RELEASE_CHANNEL").unwrap_or_else(|_| "stable".to_string());
        if !matches!(
            release_channel.as_str(),
            "stable" | "canary" | "ptb" | "development"
        ) {
            bail!("RELEASE_CHANNEL must be one of: stable, canary, ptb, development");
        }

        for key in [
            "CLIENT_VERSION",
            "BROWSER_VERSION",
            "CHROME_VERSION",
            "CLIENT_OS_VERSION",
            "SYSTEM_LOCALE",
            "BROWSER",
        ] {
            if let Ok(v) = env::var(key) {
                if v.trim().is_empty() {
                    bail!("{key} must not be empty");
                }
            }
        }

        let display_server = match env::var("DISPLAY_SERVER") {
            Ok(v) => v,
            Err(_) => {
                if os == "Linux" {
                    "wayland".to_string()
                } else {
                    "unknown".to_string()
                }
            }
        };
        if !matches!(display_server.as_str(), "wayland" | "x11" | "unknown") {
            bail!("DISPLAY_SERVER must be one of: wayland, x11, unknown");
        }
        if os != "Linux" && display_server != "unknown" {
            bail!("DISPLAY_SERVER must be 'unknown' for non-Linux OS");
        }

        let runtime_environment = match env::var("RUNTIME_ENVIRONMENT") {
            Ok(v) => v,
            Err(_) => "native".to_string(),
        };
        if !matches!(runtime_environment.as_str(), "native" | "web" | "unknown") {
            bail!("RUNTIME_ENVIRONMENT must be one of: native, web, unknown");
        }

        let distro = env::var("DISTRO").unwrap_or_else(|_| {
            if os == "Linux" {
                "CachyOS".to_string()
            } else {
                String::new()
            }
        });
        if os != "Linux" && !distro.is_empty() {
            bail!("DISTRO must be empty for non-Linux OS");
        }

        let window_manager = env::var("WINDOW_MANAGER").unwrap_or_else(|_| {
            if os == "Linux" {
                "niri,unknown".to_string()
            } else {
                String::new()
            }
        });
        if os != "Linux" && !window_manager.is_empty() {
            bail!("WINDOW_MANAGER must be empty for non-Linux OS");
        }

        // Custom UA deep validation
        let browser_user_agent = env::var("BROWSER_USER_AGENT").unwrap_or_default();
        if !browser_user_agent.is_empty() {
            let os_token = match os.as_str() {
                "Windows" => "Windows",
                "macOS" => "Macintosh",
                _ => "Linux",
            };
            if !browser_user_agent.contains(os_token) {
                bail!("BROWSER_USER_AGENT must contain '{os_token}' for CLIENT_OS={os}");
            }
            let expected_arch_token = if os == "Windows" {
                if os_arch == "x86_64" {
                    "x64"
                } else {
                    os_arch.as_str()
                }
            } else if os == "macOS" {
                if os_arch == "aarch64" {
                    "Apple Silicon"
                } else {
                    "Intel"
                }
            } else {
                os_arch.as_str()
            };
            if !browser_user_agent.contains(expected_arch_token) {
                bail!("BROWSER_USER_AGENT must contain '{expected_arch_token}' for CLIENT_OS={os}");
            }
            let client_version =
                env::var("CLIENT_VERSION").unwrap_or_else(|_| "1.0.156".to_string());
            let chrome_version =
                env::var("CHROME_VERSION").unwrap_or_else(|_| "148.0.7778.280".to_string());
            let electron_version =
                env::var("BROWSER_VERSION").unwrap_or_else(|_| "42.9.0".to_string());
            let discord_token = format!("discord/{}", client_version);
            let chrome_token = format!("Chrome/{}", chrome_version);
            let electron_token = format!("Electron/{}", electron_version);
            if !browser_user_agent.contains(&discord_token) {
                bail!("BROWSER_USER_AGENT must contain {discord_token}");
            }
            if !browser_user_agent.contains(&chrome_token) {
                bail!("BROWSER_USER_AGENT must contain {chrome_token}");
            }
            if !browser_user_agent.contains(&electron_token) {
                bail!("BROWSER_USER_AGENT must contain {electron_token}");
            }
        }

        let client_profile = ClientProfile {
            os: os.clone(),
            os_version: env::var("CLIENT_OS_VERSION").unwrap_or_else(|_| {
                if os == "Linux" {
                    "7.1.8-1-cachyos".to_string()
                } else if os == "Windows" {
                    "10.0.22631".to_string()
                } else {
                    "14.5".to_string()
                }
            }),
            os_arch: os_arch.clone(),
            app_arch: app_arch.clone(),
            system_locale: env::var("SYSTEM_LOCALE").unwrap_or_else(|_| "en-GB".to_string()),
            browser: env::var("BROWSER").unwrap_or_else(|_| "Discord Client".to_string()),
            browser_version: env::var("BROWSER_VERSION").unwrap_or_else(|_| "42.9.0".to_string()),
            chrome_version: env::var("CHROME_VERSION")
                .unwrap_or_else(|_| "148.0.7778.280".to_string()),
            browser_user_agent,
            client_version: env::var("CLIENT_VERSION").unwrap_or_else(|_| "1.0.156".to_string()),
            client_build_number: env_u64("CLIENT_BUILD_NUMBER", 606747)?,
            native_build_number: env_u64("NATIVE_BUILD_NUMBER", 89799)?,
            release_channel,
            distro,
            window_manager,
            display_server,
            runtime_environment,
            device: env::var("DEVICE").unwrap_or_default(),
            heartbeat_jitter_percent,
            jitter_stddev_percent,
            presence_status,
        };

        Ok(Self {
            accounts,
            guild_id,
            channel_id,
            port: env_u64("PORT", 7860)?
                .try_into()
                .context("PORT must fit u16")?,
            self_mute: env_bool("SELF_MUTE", true)?,
            self_deaf: env_bool("SELF_DEAF", true)?,
            startup_grace: Duration::from_secs(env_u64("STARTUP_GRACE_SECONDS", 90)?),
            health_fail_after: Duration::from_secs(env_u64("HEALTH_FAIL_AFTER_SECONDS", 180)?),
            supervisor_backoff_max: Duration::from_secs(env_u64(
                "SUPERVISOR_BACKOFF_MAX_SECONDS",
                90,
            )?),
            memory_warn_mb,
            memory_restart_mb,
            memory_restart_samples,
            client_profile,
        })
    }
}

fn env_required_id(name: &str) -> Result<String> {
    let raw = env::var(name).with_context(|| format!("{name} is required"))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() || !trimmed.chars().all(|c| c.is_ascii_digit()) {
        bail!("{name} must be a Discord snowflake integer");
    }
    Ok(trimmed.to_string())
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse()
            .with_context(|| format!("{name} must be an integer")),
        _ => Ok(default),
    }
}

fn env_f64(name: &str, default: f64) -> Result<f64> {
    match env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse()
            .with_context(|| format!("{name} must be a number")),
        _ => Ok(default),
    }
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    let Ok(raw) = env::var(name) else {
        return Ok(default);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("{name} must be true or false"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    const ENV_KEYS: &[&str] = &[
        "DISCORD_TOKENS",
        "TARGET_GUILD_ID",
        "TARGET_CHANNEL_ID",
        "CLIENT_OS",
        "CLIENT_OS_VERSION",
        "CLIENT_OS_ARCH",
        "CLIENT_APP_ARCH",
        "SYSTEM_LOCALE",
        "BROWSER",
        "BROWSER_VERSION",
        "CHROME_VERSION",
        "BROWSER_USER_AGENT",
        "CLIENT_VERSION",
        "CLIENT_BUILD_NUMBER",
        "NATIVE_BUILD_NUMBER",
        "RELEASE_CHANNEL",
        "DISTRO",
        "WINDOW_MANAGER",
        "DISPLAY_SERVER",
        "RUNTIME_ENVIRONMENT",
        "DEVICE",
        "PRESENCE_STATUS",
        "HEARTBEAT_JITTER_PERCENT",
        "JITTER_STDDEV_PERCENT",
        "SELF_MUTE",
        "SELF_DEAF",
        "PORT",
        "MAX_ACCOUNTS",
        "MEMORY_WARN_MB",
        "MEMORY_RESTART_MB",
        "MEMORY_RESTART_SAMPLES",
        "STARTUP_GRACE_SECONDS",
        "HEALTH_FAIL_AFTER_SECONDS",
        "SUPERVISOR_BACKOFF_MAX_SECONDS",
    ];

    struct EnvGuard {
        snapshot: Vec<(String, Option<String>)>,
    }

    impl EnvGuard {
        fn new() -> Self {
            let snapshot = ENV_KEYS
                .iter()
                .map(|k| (k.to_string(), std::env::var(k).ok()))
                .collect();
            Self { snapshot }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.snapshot.iter() {
                match value {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    fn clear_all_env() {
        for k in ENV_KEYS {
            std::env::remove_var(k);
        }
    }

    fn set_valid_base_env() {
        std::env::set_var("DISCORD_TOKENS", "token1,token2");
        std::env::set_var("TARGET_GUILD_ID", "123");
        std::env::set_var("TARGET_CHANNEL_ID", "456");
    }

    // Existing tests (restored)
    #[test]
    #[serial]
    fn valid_config_linux_defaults() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        let result = Config::from_env();
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.client_profile.os, "Linux");
        assert_eq!(config.client_profile.distro, "CachyOS");
        assert_eq!(config.client_profile.window_manager, "niri,unknown");
        assert_eq!(config.client_profile.display_server, "wayland");
        drop(guard);
    }

    #[test]
    #[serial]
    fn windows_defaults() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("CLIENT_OS", "Windows");
        let result = Config::from_env();
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.client_profile.distro, "");
        assert_eq!(config.client_profile.window_manager, "");
        assert_eq!(config.client_profile.display_server, "unknown");
        drop(guard);
    }

    #[test]
    #[serial]
    fn non_linux_distro_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("CLIENT_OS", "Windows");
        std::env::set_var("DISTRO", "CachyOS");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("DISTRO"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn non_linux_wayland_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("CLIENT_OS", "macOS");
        std::env::set_var("DISPLAY_SERVER", "wayland");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("DISPLAY_SERVER"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn empty_version_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("CLIENT_VERSION", "");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("CLIENT_VERSION"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn os_arch_mismatch_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("CLIENT_OS_ARCH", "x86_64");
        std::env::set_var("CLIENT_APP_ARCH", "aarch64");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("CLIENT_OS_ARCH"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn memory_samples_zero_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("MEMORY_RESTART_SAMPLES", "0");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("MEMORY_RESTART_SAMPLES"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn ua_conflict_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("CLIENT_OS", "Windows");
        std::env::set_var("BROWSER_USER_AGENT", "Mozilla/5.0 (X11; Linux x86_64)");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("BROWSER_USER_AGENT"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn ua_arch_mismatch_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("CLIENT_OS", "macOS");
        std::env::set_var("CLIENT_OS_ARCH", "aarch64");
        std::env::set_var(
            "BROWSER_USER_AGENT",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5)",
        );
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("BROWSER_USER_AGENT"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn env_restore_preserves_original_value() {
        let before = env::var("CLIENT_OS").ok();
        {
            let guard = EnvGuard::new();
            std::env::set_var("CLIENT_OS", "Windows");
            drop(guard);
        }
        let after = env::var("CLIENT_OS").ok();
        assert_eq!(after, before);
    }

    // Additional tests for memory NaN/negative
    #[test]
    #[serial]
    fn memory_nan_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("MEMORY_WARN_MB", "NaN");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("MEMORY_WARN_MB"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn memory_negative_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("MEMORY_WARN_MB", "-1");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("MEMORY_WARN_MB"));
        drop(guard);
    }

    // Restored tests from earlier revisions
    #[test]
    #[serial]
    fn invalid_presence_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("PRESENCE_STATUS", "busy");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("PRESENCE_STATUS"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn invalid_display_server_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("DISPLAY_SERVER", "gnome");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("DISPLAY_SERVER"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn invalid_runtime_environment_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("RUNTIME_ENVIRONMENT", "sandbox");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("RUNTIME_ENVIRONMENT"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn invalid_jitter_stddev_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("HEARTBEAT_JITTER_PERCENT", "20");
        std::env::set_var("JITTER_STDDEV_PERCENT", "30");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("JITTER_STDDEV_PERCENT"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn zero_stddev_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("JITTER_STDDEV_PERCENT", "0");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("JITTER_STDDEV_PERCENT"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn jitter_above_100_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("HEARTBEAT_JITTER_PERCENT", "101");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("HEARTBEAT_JITTER_PERCENT"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn malformed_guild_id_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("TARGET_GUILD_ID", "abc");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("TARGET_GUILD_ID"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn invalid_boolean_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("SELF_MUTE", "maybe");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("SELF_MUTE"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn memory_restart_less_than_warn_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("MEMORY_WARN_MB", "500");
        std::env::set_var("MEMORY_RESTART_MB", "400");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("MEMORY_RESTART_MB"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn invalid_os_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("CLIENT_OS", "FreeBSD");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("CLIENT_OS"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn invalid_os_arch_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("CLIENT_OS_ARCH", "arm");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("CLIENT_OS_ARCH"));
        drop(guard);
    }

    #[test]
    #[serial]
    fn invalid_release_channel_rejected() {
        let guard = EnvGuard::new();
        clear_all_env();
        set_valid_base_env();
        std::env::set_var("RELEASE_CHANNEL", "dev");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("RELEASE_CHANNEL"));
        drop(guard);
    }
}
