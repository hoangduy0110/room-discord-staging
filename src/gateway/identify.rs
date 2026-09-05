use crate::config::ClientProfile;
use serde_json::{json, Value};

fn build_user_agent(profile: &ClientProfile) -> String {
    if !profile.browser_user_agent.is_empty() {
        return profile.browser_user_agent.clone();
    }
    match profile.os.to_lowercase().as_str() {
        "windows" => {
            let win_arch = if profile.os_arch == "x86_64" {
                "x64".to_string()
            } else {
                profile.os_arch.clone()
            };
            format!(
                "Mozilla/5.0 (Windows NT {}; Win64; {}) AppleWebKit/537.36 (KHTML, like Gecko) discord/{} Chrome/{} Electron/{} Safari/537.36",
                profile.os_version,
                win_arch,
                profile.client_version,
                profile.chrome_version,
                profile.browser_version
            )
        }
        "macos" => {
            let mac_os_version = profile.os_version.replace('.', "_");
            if profile.os_arch == "aarch64" {
                format!(
                    "Mozilla/5.0 (Macintosh; Apple Silicon Mac OS X {}) AppleWebKit/537.36 (KHTML, like Gecko) discord/{} Chrome/{} Electron/{} Safari/537.36",
                    mac_os_version,
                    profile.client_version,
                    profile.chrome_version,
                    profile.browser_version
                )
            } else {
                format!(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X {}) AppleWebKit/537.36 (KHTML, like Gecko) discord/{} Chrome/{} Electron/{} Safari/537.36",
                    mac_os_version,
                    profile.client_version,
                    profile.chrome_version,
                    profile.browser_version
                )
            }
        }
        "linux" => format!(
            "Mozilla/5.0 (X11; Linux {}) AppleWebKit/537.36 (KHTML, like Gecko) discord/{} Chrome/{} Electron/{} Safari/537.36",
            profile.os_arch,
            profile.client_version,
            profile.chrome_version,
            profile.browser_version
        ),
        _ => format!(
            "Mozilla/5.0 ({}) AppleWebKit/537.36 (KHTML, like Gecko) discord/{} Chrome/{} Electron/{} Safari/537.36",
            profile.os,
            profile.client_version,
            profile.chrome_version,
            profile.browser_version
        ),
    }
}

pub fn identify(token: &str, profile: &ClientProfile) -> Value {
    let ua = build_user_agent(profile);

    let properties = json!({
        "os": profile.os,
        "browser": profile.browser,
        "device": profile.device,
        "system_locale": profile.system_locale,
        "browser_user_agent": ua,
        "browser_version": profile.browser_version,
        "chrome_version": profile.chrome_version,
        "os_version": profile.os_version,
        "os_arch": profile.os_arch,
        "app_arch": profile.app_arch,
        "distro": profile.distro,
        "window_manager": profile.window_manager,
        "display_server": profile.display_server,
        "runtime_environment": profile.runtime_environment,
        "release_channel": profile.release_channel,
        "client_build_number": profile.client_build_number,
        "client_version": profile.client_version,
        "native_build_number": profile.native_build_number,
        "has_client_mods": false,
        "client_event_source": Value::Null
    });

    let common_client_properties = properties.clone();

    let presence = match profile.presence_status.as_str() {
        "online" => json!({"status": "online", "since": 0, "activities": [], "afk": false}),
        "idle" => json!({"status": "idle", "since": 0, "activities": [], "afk": false}),
        "dnd" => json!({"status": "dnd", "since": 0, "activities": [], "afk": false}),
        _ => json!({"status": "invisible", "since": 0, "activities": [], "afk": true}),
    };

    json!({
        "op": 2,
        "d": {
            "token": token,
            "capabilities": 16381,
            "properties": properties,
            "presence": presence,
            "compress": false,
            "client_state": {
                "guild_versions": {},
                "highest_last_message_id": "0",
                "read_state_version": 0,
                "user_guild_settings_version": -1,
                "user_settings_version": -1,
                "private_channels_version": "0",
                "api_code_version": 0
            },
            "common_client_properties": common_client_properties
        }
    })
}

pub fn heartbeat(seq: Option<u64>) -> Value {
    json!({"op": 1, "d": seq})
}

pub fn voice_state(guild_id: &str, channel_id: Option<&str>, mute: bool, deaf: bool) -> Value {
    json!({
        "op": 4,
        "d": {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "self_mute": mute,
            "self_deaf": deaf,
            "self_video": false
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientProfile;

    fn base_profile() -> ClientProfile {
        ClientProfile {
            os: "Linux".into(),
            os_version: "7.1.8-1-cachyos".into(),
            os_arch: "x86_64".into(),
            app_arch: "x86_64".into(),
            system_locale: "en-GB".into(),
            browser: "Discord Client".into(),
            browser_version: "42.9.0".into(),
            chrome_version: "148.0.7778.280".into(),
            browser_user_agent: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) discord/1.0.156 Chrome/148.0.7778.280 Electron/42.9.0 Safari/537.36".into(),
            client_version: "1.0.156".into(),
            client_build_number: 606747,
            native_build_number: 89799,
            release_channel: "stable".into(),
            distro: "CachyOS".into(),
            window_manager: "niri,unknown".into(),
            display_server: "wayland".into(),
            runtime_environment: "native".into(),
            device: "".into(),
            heartbeat_jitter_percent: 10,
            jitter_stddev_percent: 3,
            presence_status: "invisible".into(),
        }
    }

    #[test]
    fn identify_contains_capabilities_and_common_properties() {
        let profile = base_profile();
        let payload = identify("token", &profile);
        assert!(payload["d"]["capabilities"].is_number());
        assert!(payload["d"]["common_client_properties"].is_object());
        assert!(payload["d"]["properties"].is_object());
    }

    #[test]
    fn properties_equals_common_properties() {
        let profile = base_profile();
        let payload = identify("token", &profile);
        assert_eq!(payload["d"]["properties"], payload["d"]["common_client_properties"]);
    }

    #[test]
    fn fallback_ua_linux() {
        let mut profile = base_profile();
        profile.browser_user_agent = String::new();
        let payload = identify("token", &profile);
        let ua = payload["d"]["properties"]["browser_user_agent"].as_str().unwrap();
        assert!(ua.contains("X11; Linux x86_64"));
        assert!(ua.contains("Chrome/148.0.7778.280"));
        assert!(ua.contains("Electron/42.9.0"));
        assert!(ua.contains("discord/1.0.156"));
    }

    #[test]
    fn fallback_ua_windows() {
        let mut profile = base_profile();
        profile.os = "Windows".into();
        profile.os_version = "10.0.22631".into();
        profile.os_arch = "x86_64".into();
        profile.browser_user_agent = String::new();
        let payload = identify("token", &profile);
        let ua = payload["d"]["properties"]["browser_user_agent"].as_str().unwrap();
        assert!(ua.contains("Windows NT 10.0.22631"));
        assert!(ua.contains("Win64; x64"));
        assert!(!ua.contains("Linux"));
    }

    #[test]
    fn fallback_ua_macos_intel() {
        let mut profile = base_profile();
        profile.os = "macOS".into();
        profile.os_version = "14.5".into();
        profile.os_arch = "x86_64".into();
        profile.browser_user_agent = String::new();
        let payload = identify("token", &profile);
        let ua = payload["d"]["properties"]["browser_user_agent"].as_str().unwrap();
        assert!(ua.contains("Macintosh; Intel Mac OS X 14_5"));
        assert!(!ua.contains("Linux"));
    }

    #[test]
    fn fallback_ua_macos_arm() {
        let mut profile = base_profile();
        profile.os = "macOS".into();
        profile.os_version = "14.5".into();
        profile.os_arch = "aarch64".into();
        profile.browser_user_agent = String::new();
        let payload = identify("token", &profile);
        let ua = payload["d"]["properties"]["browser_user_agent"].as_str().unwrap();
        assert!(ua.contains("Apple Silicon Mac OS X 14_5"));
        assert!(!ua.contains("Intel"));
    }

    #[test]
    fn explicit_ua_preserved() {
        let profile = base_profile();
        let payload = identify("token", &profile);
        let ua = payload["d"]["properties"]["browser_user_agent"].as_str().unwrap();
        assert_eq!(ua, profile.browser_user_agent);
    }
}