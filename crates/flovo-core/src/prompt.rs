//! Prompt 模板工厂模块
//!
//! 使用 tera 模板引擎从文件加载 Prompt 模板，支持动态参数渲染。
//! 对应 Python 版本的 `python_example/workflow/prompt.py`。

use serde_json::Value;
use std::path::{Path, PathBuf};
use tera::{Context, Tera};

use crate::error::{Result, WorkflowError};

// ─── 错误转换 ────────────────────────────────────────────────────────────────

impl From<tera::Error> for WorkflowError {
    fn from(e: tera::Error) -> Self {
        WorkflowError::Other(format!("Template error: {e}"))
    }
}

// ─── Prompt 类型枚举 ─────────────────────────────────────────────────────────

/// Prompt 类型，对应 Python 版本的三种 Prompt 子类
#[derive(Debug, Clone, PartialEq)]
pub enum PromptKind {
    /// 普通同步聊天
    Chat,
    /// 流式聊天
    ChatStream,
    /// 意图分析
    IntentAnalysis,
}

// ─── PromptBase ──────────────────────────────────────────────────────────────

/// 渲染后的 Prompt 数据，包含类型和最终文本
///
/// 对应 Python 的 `PromptBase` 及其子类。
#[derive(Debug, Clone)]
pub struct PromptBase {
    /// Prompt 类型
    pub kind: PromptKind,
    /// 渲染后的完整 Prompt 文本
    pub content: String,
}

impl PromptBase {
    fn new(kind: PromptKind, content: String) -> Self {
        Self { kind, content }
    }

    /// 返回 Prompt 文本内容
    pub fn as_str(&self) -> &str {
        &self.content
    }
}

// ─── PromptFactory ───────────────────────────────────────────────────────────

/// Prompt 模板工厂
///
/// 负责从指定目录加载 Jinja2/Tera 模板文件，并根据传入参数渲染 Prompt。
///
/// # 示例
/// ```no_run
/// use flovo_core::prompt::{PromptFactory, PromptKind};
/// use serde_json::json;
///
/// let factory = PromptFactory::new("prompt/ws").unwrap();
/// let params = json!({ "text": "我有点累了", "background": { "total_rep": 10, "current_rep": 7 } });
/// let prompt = factory.build_prompt(PromptKind::Chat, "fatigue_decision1.md", &params).unwrap();
/// println!("{}", prompt.as_str());
/// ```
pub struct PromptFactory {
    /// 模板目录路径
    template_dir: PathBuf,
    /// Tera 模板引擎实例（懒加载，每次 build 时按需渲染）
    tera: Tera,
}

impl PromptFactory {
    /// 创建 PromptFactory，从指定目录加载所有 `.md` 模板文件。
    ///
    /// # 参数
    /// - `template_dir`: 模板文件所在目录路径
    ///
    /// # 错误
    /// - 目录不存在或无法读取时返回 `WorkflowError::IoError`
    /// - 模板语法错误时返回 `WorkflowError::Other`
    pub fn new<P: AsRef<Path>>(template_dir: P) -> Result<Self> {
        let template_dir = template_dir.as_ref().to_path_buf();

        if !template_dir.exists() {
            return Err(WorkflowError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Template directory not found: {}", template_dir.display()),
            )));
        }

        // 加载目录下所有 .md 文件作为模板
        let glob_pattern = format!("{}/**/*.md", template_dir.display());
        let tera = Tera::new(&glob_pattern).map_err(|e| {
            WorkflowError::Other(format!(
                "Failed to load templates from '{}': {e}",
                template_dir.display()
            ))
        })?;

        Ok(Self { template_dir, tera })
    }

    /// 重新加载模板目录（模板文件变更后调用）
    pub fn reload(&mut self) -> Result<()> {
        let glob_pattern = format!("{}/**/*.md", self.template_dir.display());
        self.tera = Tera::new(&glob_pattern)?;
        Ok(())
    }

    /// 构建并渲染指定类型的 Prompt。
    ///
    /// # 参数
    /// - `kind`: Prompt 类型（Chat / ChatStream / IntentAnalysis）
    /// - `template_name`: 模板文件名（相对于模板目录，如 `"fatigue_decision1.md"`）
    /// - `params`: 渲染参数，JSON 对象，模板中通过 `{{ params.xxx }}` 访问
    ///
    /// # 返回
    /// 渲染后的 `PromptBase`，包含类型和完整文本。
    ///
    /// # 错误
    /// - 模板文件不存在时返回错误
    /// - 参数缺失且模板未设默认值时返回 tera 渲染错误
    pub fn build_prompt(
        &self,
        kind: PromptKind,
        template_name: &str,
        params: &Value,
    ) -> Result<PromptBase> {
        let mut ctx = Context::new();
        ctx.insert("params", params);

        let content = self.tera.render(template_name, &ctx).map_err(|e| {
            // 区分"模板不存在"和"渲染失败"两种错误
            let msg = e.to_string();
            if msg.contains("not found") || msg.contains("Template") {
                WorkflowError::Other(format!(
                    "Template '{}' not found in '{}': {e}",
                    template_name,
                    self.template_dir.display()
                ))
            } else {
                WorkflowError::Other(format!("Failed to render template '{template_name}': {e}"))
            }
        })?;

        Ok(PromptBase::new(kind, content))
    }

    /// 便捷方法：构建普通聊天 Prompt
    pub fn chat(&self, template_name: &str, params: &Value) -> Result<PromptBase> {
        self.build_prompt(PromptKind::Chat, template_name, params)
    }

    /// 便捷方法：构建流式聊天 Prompt
    pub fn chat_stream(&self, template_name: &str, params: &Value) -> Result<PromptBase> {
        self.build_prompt(PromptKind::ChatStream, template_name, params)
    }

    /// 便捷方法：构建意图分析 Prompt
    pub fn intent_analysis(&self, template_name: &str, params: &Value) -> Result<PromptBase> {
        self.build_prompt(PromptKind::IntentAnalysis, template_name, params)
    }

    /// 返回模板目录路径
    pub fn template_dir(&self) -> &Path {
        &self.template_dir
    }

    /// 列出已加载的所有模板名称
    pub fn template_names(&self) -> Vec<&str> {
        self.tera.get_template_names().collect()
    }
}

// ─── 单元测试 ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// 创建临时模板目录并写入测试模板
    fn setup_template_dir() -> TempDir {
        let dir = tempfile::tempdir().expect("create tempdir");
        fs::write(
            dir.path().join("hello.md"),
            "你好，{{ params.name }}！剩余次数：{{ params.remaining }}",
        )
        .unwrap();
        fs::write(dir.path().join("intent.md"), "用户说：{{ params.text }}").unwrap();
        dir
    }

    #[test]
    fn test_factory_new_ok() {
        let dir = setup_template_dir();
        let factory = PromptFactory::new(dir.path());
        assert!(factory.is_ok());
    }

    #[test]
    fn test_factory_new_missing_dir() {
        let result = PromptFactory::new("/nonexistent/path/xyz");
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(
            msg.contains("not found") || msg.contains("Not found"),
            "got: {msg}"
        );
    }

    #[test]
    fn test_build_prompt_chat() {
        let dir = setup_template_dir();
        let factory = PromptFactory::new(dir.path()).unwrap();
        let params = serde_json::json!({ "name": "张三", "remaining": 3 });
        let prompt = factory.chat("hello.md", &params).unwrap();
        assert_eq!(prompt.kind, PromptKind::Chat);
        assert!(prompt.content.contains("张三"));
        assert!(prompt.content.contains("3"));
    }

    #[test]
    fn test_build_prompt_chat_stream() {
        let dir = setup_template_dir();
        let factory = PromptFactory::new(dir.path()).unwrap();
        let params = serde_json::json!({ "name": "李四", "remaining": 5 });
        let prompt = factory.chat_stream("hello.md", &params).unwrap();
        assert_eq!(prompt.kind, PromptKind::ChatStream);
    }

    #[test]
    fn test_build_prompt_intent_analysis() {
        let dir = setup_template_dir();
        let factory = PromptFactory::new(dir.path()).unwrap();
        let params = serde_json::json!({ "text": "我有点累了" });
        let prompt = factory.intent_analysis("intent.md", &params).unwrap();
        assert_eq!(prompt.kind, PromptKind::IntentAnalysis);
        assert!(prompt.content.contains("我有点累了"));
    }

    #[test]
    fn test_template_not_found() {
        let dir = setup_template_dir();
        let factory = PromptFactory::new(dir.path()).unwrap();
        let params = serde_json::json!({});
        let result = factory.chat("nonexistent.md", &params);
        assert!(result.is_err());
    }

    #[test]
    fn test_template_names() {
        let dir = setup_template_dir();
        let factory = PromptFactory::new(dir.path()).unwrap();
        let names = factory.template_names();
        assert_eq!(names.len(), 2);
    }
}
