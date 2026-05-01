use core::{
    default::Default,
    fmt::{self, Write},
};
use std::collections::HashMap;

use tracing::field::{Field, Visit};

pub struct StringRecorder {
    pub display: String,
    pub is_following_args: bool,
    pub show_fields: bool,
    pub fields: HashMap<String, String>,
    pub message: Option<String>,
}
impl StringRecorder {
    pub fn new(show_fields: bool) -> Self {
        StringRecorder {
            show_fields,
            ..Default::default()
        }
    }
}

impl Visit for StringRecorder {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            let message = format!("{value:?}");
            self.message = Some(trim_debug_message(&message).to_string());
            if !self.display.is_empty() {
                self.display = format!("{value:?}\n{}", self.display)
            } else {
                self.display = format!("{value:?}")
            }
        } else if self.show_fields {
            if self.is_following_args {
                // following args
                writeln!(self.display).unwrap();
            } else {
                // first arg
                write!(self.display, " ").unwrap();
                self.is_following_args = true;
            }
            write!(self.display, "{} = {:?};", field.name(), value).unwrap();
            self.fields
                .insert(field.name().to_owned(), format!("{value:?}"));
        }
    }
}

impl core::fmt::Display for StringRecorder {
    fn fmt(&self, mut f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if !self.display.is_empty() {
            write!(&mut f, " {}", self.display)
        } else {
            Ok(())
        }
    }
}

impl core::default::Default for StringRecorder {
    fn default() -> Self {
        Self {
            display: String::new(),
            is_following_args: false,
            show_fields: true,
            fields: HashMap::new(),
            message: None,
        }
    }
}

fn trim_debug_message(message: &str) -> &str {
    message
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(message)
}
