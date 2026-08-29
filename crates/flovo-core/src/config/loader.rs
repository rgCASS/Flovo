//! 通用环境变量配置读取器。

/// 环境变量配置加载器。
#[derive(Debug, Default, Clone, Copy)]
pub struct ConfigLoader;

impl ConfigLoader {
    /// 创建加载器。环境变量由宿主进程负责注入。
    pub fn load_from_env() -> Self {
        Self
    }

    /// 读取指定环境变量，空白字符串按未配置处理。
    pub fn get(&self, key: &str) -> Option<String> {
        std::env::var(key)
            .ok()
            .and_then(|value| Self::normalize_value(&value))
    }

    fn normalize_value(raw: &str) -> Option<String> {
        let trimmed = raw.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::ConfigLoader;

    #[test]
    fn reads_trimmed_values() {
        std::env::set_var("FLOVO_TEST_VALUE", "  value  ");
        assert_eq!(
            ConfigLoader::load_from_env().get("FLOVO_TEST_VALUE"),
            Some("value".to_string())
        );
        std::env::remove_var("FLOVO_TEST_VALUE");
    }
}
