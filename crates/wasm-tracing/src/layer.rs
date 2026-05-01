#[cfg(target_arch = "wasm32")]
use std::cell::RefCell;
use std::{
    collections::HashMap,
    mem,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Mutex,
    },
};

#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tracing::{Level, Subscriber};
#[cfg(feature = "tracing-log")]
use tracing_log::NormalizeEvent as _;
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

use crate::{
    debug1, debug4, error1, error4, log1, log4, mark, mark_name, measure, prelude::*,
    recorder::StringRecorder, thread_display_suffix, warn1, warn4,
};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsCast;

struct SpanTiming {
    start_unix_nano: String,
}

const MAX_TRACE_ENTRIES: usize = 5_000;

static TRACE_ENTRY_COLLECTION_ENABLED: AtomicBool = AtomicBool::new(false);
static TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING: AtomicBool = AtomicBool::new(false);
static TRACE_ENTRY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static DROPPED_TRACE_ENTRY_COUNT: AtomicU64 = AtomicU64::new(0);
static TRACE_ENTRIES: Mutex<Vec<TraceEntry>> = Mutex::new(Vec::new());

#[cfg(target_arch = "wasm32")]
thread_local! {
    static TRACE_ENTRY_DRAIN_CALLBACK: RefCell<Option<js_sys::Function>> = RefCell::new(None);
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum TraceEntry {
    Span {
        sequence: u64,
        name: String,
        target: String,
        level: String,
        start_unix_nano: String,
        end_unix_nano: String,
        fields: HashMap<String, String>,
    },
    Log {
        sequence: u64,
        target: String,
        level: String,
        timestamp_unix_nano: String,
        message: String,
        fields: HashMap<String, String>,
    },
    Dropped {
        count: u64,
    },
}

/// Enable or disable collection of buffered tracing entries for JavaScript drains.
pub fn set_trace_entry_collection_enabled(enabled: bool) {
    TRACE_ENTRY_COLLECTION_ENABLED.store(enabled, Ordering::Relaxed);
    if !enabled {
        clear_trace_entries();
    }
}

/// Subscribe to notifications that buffered tracing entries are ready to drain.
#[cfg(target_arch = "wasm32")]
pub fn subscribe_trace_entries(callback: js_sys::Function) -> js_sys::Function {
    TRACE_ENTRY_DRAIN_CALLBACK.with(|slot| {
        *slot.borrow_mut() = Some(callback);
    });
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING.store(false, Ordering::Relaxed);

    if has_trace_entries_to_drain() {
        notify_trace_entries_ready();
    }

    let unsubscribe = Closure::<dyn FnMut()>::wrap(Box::new(|| {
        clear_trace_entry_drain_callback();
    }));
    unsubscribe.into_js_value().unchecked_into()
}

/// Drain buffered tracing entries into a JavaScript value.
#[cfg(target_arch = "wasm32")]
pub fn drain_trace_entries() -> JsValue {
    serde_wasm_bindgen::to_value(&drain_trace_entries_internal()).unwrap_or(JsValue::NULL)
}

fn trace_entry_collection_enabled() -> bool {
    TRACE_ENTRY_COLLECTION_ENABLED.load(Ordering::Relaxed)
}

fn next_trace_entry_sequence() -> u64 {
    TRACE_ENTRY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

fn push_trace_entry(entry: TraceEntry) {
    if !trace_entry_collection_enabled() {
        return;
    }

    {
        let mut entries = TRACE_ENTRIES.lock().expect("trace entries mutex poisoned");
        if entries.len() >= MAX_TRACE_ENTRIES {
            DROPPED_TRACE_ENTRY_COUNT.fetch_add(1, Ordering::Relaxed);
        } else {
            entries.push(entry);
        }
    }

    notify_trace_entries_ready();
}

#[allow(dead_code)]
fn drain_trace_entries_internal() -> Vec<TraceEntry> {
    let mut entries = TRACE_ENTRIES.lock().expect("trace entries mutex poisoned");
    let mut drained = mem::take(&mut *entries);
    let dropped = DROPPED_TRACE_ENTRY_COUNT.swap(0, Ordering::Relaxed);
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING.store(false, Ordering::Release);
    if dropped > 0 {
        drained.push(TraceEntry::Dropped { count: dropped });
    }
    drained
}

#[cfg(target_arch = "wasm32")]
fn has_trace_entries_to_drain() -> bool {
    !TRACE_ENTRIES
        .lock()
        .expect("trace entries mutex poisoned")
        .is_empty()
        || DROPPED_TRACE_ENTRY_COUNT.load(Ordering::Relaxed) > 0
}

#[cfg(target_arch = "wasm32")]
fn notify_trace_entries_ready() {
    if TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING.load(Ordering::Acquire) {
        return;
    }

    let callback = TRACE_ENTRY_DRAIN_CALLBACK.with(|slot| slot.borrow().clone());
    let Some(callback) = callback else {
        return;
    };

    if TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING.swap(true, Ordering::AcqRel) {
        return;
    }

    if callback.call0(&JsValue::UNDEFINED).is_err() {
        TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING.store(false, Ordering::Release);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn notify_trace_entries_ready() {}

#[cfg(target_arch = "wasm32")]
fn clear_trace_entry_drain_callback() {
    TRACE_ENTRY_DRAIN_CALLBACK.with(|slot| {
        *slot.borrow_mut() = None;
    });
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING.store(false, Ordering::Release);
}

fn clear_trace_entries() {
    TRACE_ENTRIES
        .lock()
        .expect("trace entries mutex poisoned")
        .clear();
    DROPPED_TRACE_ENTRY_COUNT.store(0, Ordering::Relaxed);
    TRACE_ENTRY_SEQUENCE.store(0, Ordering::Relaxed);
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING.store(false, Ordering::Release);
}

#[cfg(test)]
fn push_trace_entry_for_test(entry: TraceEntry) {
    push_trace_entry(entry);
}

#[cfg(test)]
fn drain_trace_entries_for_test() -> Vec<TraceEntry> {
    drain_trace_entries_internal()
}

#[cfg(test)]
fn clear_trace_entries_for_test() {
    clear_trace_entries();
}

#[cfg(test)]
fn set_trace_entry_drain_notification_pending_for_test(pending: bool) {
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING.store(pending, Ordering::Relaxed);
}

#[cfg(test)]
fn trace_entry_drain_notification_pending_for_test() -> bool {
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING.load(Ordering::Relaxed)
}

#[doc = r#"
Implements [tracing_subscriber::layer::Layer] which uses [wasm_bindgen] for marking and measuring via `window.performance` and `window.console`

If composing a subscriber, provide `WasmLayer` as such:

```notest
use tracing_subscriber::prelude::*;
use tracing::Subscriber;

pub struct MySubscriber {
    // ...
}

impl Subscriber for MySubscriber {
    // ...
}

let subscriber = MySubscriber::new()
    .with(WasmLayer::default());

tracing::subscriber::set_global_default(subscriber);
```
"#]
pub struct WasmLayer {
    last_event_id: AtomicUsize,
    config: WasmLayerConfig,
}

impl WasmLayer {
    /// Create a new [Layer] with the provided config
    pub fn new(config: WasmLayerConfig) -> Self {
        WasmLayer {
            last_event_id: AtomicUsize::new(0),
            config,
        }
    }
}

impl Default for WasmLayer {
    fn default() -> Self {
        WasmLayer::new(WasmLayerConfig::default())
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for WasmLayer {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        let level = metadata.level();
        level <= &self.config.max_level
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: Context<'_, S>,
    ) {
        let mut new_debug_record = StringRecorder::new(self.config.show_fields);
        attrs.record(&mut new_debug_record);

        if let Some(span_ref) = ctx.span(id) {
            span_ref
                .extensions_mut()
                .insert::<StringRecorder>(new_debug_record);
        }
    }

    fn on_record(&self, id: &tracing::Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        if let Some(span_ref) = ctx.span(id) {
            if let Some(debug_record) = span_ref.extensions_mut().get_mut::<StringRecorder>() {
                values.record(debug_record);
            }
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let should_collect_entry = trace_entry_collection_enabled();
        if !self.config.enabled && !should_collect_entry {
            return;
        }

        let mut recorder = StringRecorder::new(self.config.show_fields);
        event.record(&mut recorder);
        #[cfg(feature = "tracing-log")]
        let normalized_meta = event.normalized_metadata();
        #[cfg(feature = "tracing-log")]
        let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());
        #[cfg(not(feature = "tracing-log"))]
        let meta = event.metadata();
        let level = meta.level();
        if should_collect_entry {
            record_log_trace_entry(meta, level, &recorder);
        }

        if !self.config.enabled {
            return;
        }

        if self.config.report_logs_in_timings {
            let mark_name = format!(
                "c{:x}",
                self.last_event_id
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed)
            );
            // mark and measure so you can see a little blip in the profile
            mark(&mark_name);
            let _ = measure(
                format!(
                    "{} {}{} {}",
                    level,
                    meta.module_path().unwrap_or("..."),
                    thread_display_suffix(),
                    recorder,
                ),
                mark_name,
            );
        }

        let origin = if self.config.show_origin {
            meta.file()
                .and_then(|file| {
                    meta.line().map(|ln| {
                        format!(
                            "{}{}:{}",
                            self.config.origin_base_url.as_deref().unwrap_or_default(),
                            file,
                            ln
                        )
                    })
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        let fields = ctx
            .lookup_current()
            .and_then(|span| {
                span.extensions()
                    .get::<StringRecorder>()
                    .map(|span_recorder| {
                        span_recorder
                            .fields
                            .iter()
                            .map(|(key, value)| format!("\n\t{key}: {value}"))
                            .collect::<Vec<_>>()
                            .join("")
                    })
            })
            .unwrap_or_default();
        if self.config.color {
            log_with_color(
                format!(
                    "%c{}%c {}{}%c{}{}",
                    level,
                    origin,
                    thread_display_suffix(),
                    recorder,
                    fields
                ),
                level,
                self.config.use_console_methods,
            );
        } else {
            log(
                format!(
                    "{} {}{} {}{}",
                    level,
                    origin,
                    thread_display_suffix(),
                    recorder,
                    fields
                ),
                level,
                self.config.use_console_methods,
            );
        }
    }

    fn on_enter(&self, id: &tracing::Id, ctx: Context<'_, S>) {
        if self.config.report_logs_in_timings {
            mark(&mark_name(id));
        }

        if trace_entry_collection_enabled() {
            if let Some(span_ref) = ctx.span(id) {
                span_ref.extensions_mut().insert(SpanTiming {
                    start_unix_nano: unix_nano_now_string(),
                });
            }
        }

        if self.config.console_group_spans {
            if let Some(span_ref) = ctx.span(id) {
                let meta = span_ref.metadata();
                let level = meta.level();
                let fields = span_ref
                    .extensions()
                    .get::<StringRecorder>()
                    .map(|r| r.to_string())
                    .unwrap_or_default();
                let message =
                    format!("▶ \"{}\"{}{}", meta.name(), thread_display_suffix(), fields,);
                if self.config.color {
                    log_with_color(
                        format!("%c{}%c {}", level, message),
                        level,
                        self.config.use_console_methods,
                    );
                } else {
                    log(
                        format!("{} {}", level, message),
                        level,
                        self.config.use_console_methods,
                    );
                }
            }
        }
    }

    fn on_exit(&self, id: &tracing::Id, ctx: Context<'_, S>) {
        let should_collect_entry = trace_entry_collection_enabled();
        if !self.config.report_logs_in_timings && !should_collect_entry {
            return;
        }

        if let Some(span_ref) = ctx.span(id) {
            let meta = span_ref.metadata();
            let extensions = span_ref.extensions();
            let debug_record = extensions.get::<StringRecorder>();
            let timing = extensions.get::<SpanTiming>();
            if let Some(debug_record) = debug_record {
                if self.config.report_logs_in_timings {
                    let _ = measure(
                        format!(
                            "\"{}\"{} {} {}",
                            meta.name(),
                            thread_display_suffix(),
                            meta.module_path().unwrap_or("..."),
                            debug_record,
                        ),
                        mark_name(id),
                    );
                }
                record_span_trace_entry(meta, Some(debug_record), timing);
            } else {
                if self.config.report_logs_in_timings {
                    let _ = measure(
                        format!(
                            "\"{}\"{} {}",
                            meta.name(),
                            thread_display_suffix(),
                            meta.module_path().unwrap_or("..."),
                        ),
                        mark_name(id),
                    );
                }
                record_span_trace_entry(meta, None, timing);
            }
        }
    }
}

fn record_span_trace_entry(
    meta: &tracing::Metadata<'_>,
    recorder: Option<&StringRecorder>,
    timing: Option<&SpanTiming>,
) {
    if !trace_entry_collection_enabled() {
        return;
    }

    let end_unix_nano = unix_nano_now_string();
    push_trace_entry(TraceEntry::Span {
        sequence: next_trace_entry_sequence(),
        name: meta.name().to_string(),
        target: meta.target().to_string(),
        level: meta.level().to_string(),
        start_unix_nano: timing
            .map(|value| value.start_unix_nano.clone())
            .unwrap_or_else(|| end_unix_nano.clone()),
        end_unix_nano,
        fields: recorder
            .map(|value| value.fields.clone())
            .unwrap_or_default(),
    });
}

fn record_log_trace_entry(meta: &tracing::Metadata<'_>, level: &Level, recorder: &StringRecorder) {
    if !trace_entry_collection_enabled() {
        return;
    }

    push_trace_entry(TraceEntry::Log {
        sequence: next_trace_entry_sequence(),
        target: meta.target().to_string(),
        level: level.to_string(),
        timestamp_unix_nano: unix_nano_now_string(),
        message: recorder
            .message
            .clone()
            .unwrap_or_else(|| recorder.display.trim().to_string()),
        fields: recorder.fields.clone(),
    });
}

fn unix_nano_now_string() -> String {
    #[cfg(target_arch = "wasm32")]
    {
        format!("{:.0}", js_sys::Date::now() * 1_000_000.0)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        nanos.to_string()
    }
}

fn log(message: String, level: &Level, use_console_methods: bool) {
    if use_console_methods {
        match *level {
            Level::TRACE | Level::DEBUG => debug1(message),
            Level::INFO => log1(message),
            Level::WARN => warn1(message),
            Level::ERROR => error1(message),
        }
    } else {
        log1(message)
    }
}

fn log_with_color(message: String, level: &Level, use_console_methods: bool) {
    let level_log = if use_console_methods {
        match *level {
            Level::TRACE | Level::DEBUG => debug4,
            Level::INFO => log4,
            Level::WARN => warn4,
            Level::ERROR => error4,
        }
    } else {
        log4
    };
    level_log(
        message,
        level.color(),
        "color: gray; font-style: italic",
        "color: inherit",
    );
}

trait LevelExt {
    fn color(&self) -> &'static str;
}

impl LevelExt for Level {
    fn color(&self) -> &'static str {
        match *self {
            Level::TRACE => "color: dodgerblue; background: #444",
            Level::DEBUG => "color: lawngreen; background: #444",
            Level::INFO => "color: whitesmoke; background: #444",
            Level::WARN => "color: orange; background: #444",
            Level::ERROR => "color: red; background: #444",
        }
    }
}

#[cfg(test)]
mod trace_entry_tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use tracing_subscriber::prelude::*;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn span_entry(sequence: u64) -> TraceEntry {
        TraceEntry::Span {
            sequence,
            name: "opfs put".to_string(),
            target: "opfs_btree::db".to_string(),
            level: "TRACE".to_string(),
            start_unix_nano: "1775000000000000000".to_string(),
            end_unix_nano: "1775000000000001000".to_string(),
            fields: HashMap::from([("key_len".to_string(), "8".to_string())]),
        }
    }

    fn log_entry(sequence: u64) -> TraceEntry {
        TraceEntry::Log {
            sequence,
            target: "opfs_btree::db".to_string(),
            level: "WARN".to_string(),
            timestamp_unix_nano: "1775000000000002000".to_string(),
            message: "retrying write".to_string(),
            fields: HashMap::from([("attempt".to_string(), "2".to_string())]),
        }
    }

    fn emit_test_trace_records() {
        tracing::warn!(target: "opfs_btree::db", attempt = 2, "retrying write");

        let span = tracing::span!(
            target: "opfs_btree::db",
            Level::TRACE,
            "opfs put",
            key_len = 8
        );
        let _entered = span.enter();
    }

    fn silent_test_layer() -> WasmLayer {
        WasmLayer::new(WasmLayerConfig::new().disable().remove_timings())
    }

    #[test]
    fn ignores_entries_while_collection_is_disabled() {
        let _guard = TEST_LOCK.lock().unwrap();
        clear_trace_entries_for_test();
        set_trace_entry_collection_enabled(false);

        let subscriber = tracing_subscriber::registry().with(silent_test_layer());
        tracing::subscriber::with_default(subscriber, emit_test_trace_records);

        assert!(drain_trace_entries_for_test().is_empty());
    }

    #[test]
    fn enabling_collection_buffers_layer_spans_and_logs() {
        let _guard = TEST_LOCK.lock().unwrap();
        clear_trace_entries_for_test();
        set_trace_entry_collection_enabled(true);

        let subscriber = tracing_subscriber::registry().with(silent_test_layer());
        tracing::subscriber::with_default(subscriber, emit_test_trace_records);

        let drained = drain_trace_entries_for_test();
        assert_eq!(drained.len(), 2);
        assert!(matches!(
            &drained[0],
            TraceEntry::Log {
                sequence: 0,
                target,
                level,
                message,
                fields,
                ..
            } if target == "opfs_btree::db"
                && level == "WARN"
                && message == "retrying write"
                && fields.get("attempt") == Some(&"2".to_string())
        ));
        assert!(matches!(
            &drained[1],
            TraceEntry::Span {
                sequence: 1,
                name,
                target,
                level,
                fields,
                ..
            } if name == "opfs put"
                && target == "opfs_btree::db"
                && level == "TRACE"
                && fields.get("key_len") == Some(&"8".to_string())
        ));
    }

    #[test]
    fn drain_returns_entries_and_clears_buffer() {
        let _guard = TEST_LOCK.lock().unwrap();
        clear_trace_entries_for_test();
        set_trace_entry_collection_enabled(true);
        set_trace_entry_drain_notification_pending_for_test(true);

        push_trace_entry_for_test(span_entry(0));
        push_trace_entry_for_test(log_entry(1));

        assert_eq!(
            drain_trace_entries_for_test(),
            vec![span_entry(0), log_entry(1)]
        );
        assert!(!trace_entry_drain_notification_pending_for_test());
        assert!(drain_trace_entries_for_test().is_empty());
    }

    #[test]
    fn overflowing_the_buffer_reports_dropped_entries() {
        let _guard = TEST_LOCK.lock().unwrap();
        clear_trace_entries_for_test();
        set_trace_entry_collection_enabled(true);

        for index in 0..=MAX_TRACE_ENTRIES {
            push_trace_entry_for_test(span_entry(index as u64));
        }

        let drained = drain_trace_entries_for_test();
        assert_eq!(drained.len(), MAX_TRACE_ENTRIES + 1);
        assert!(matches!(
            drained.last(),
            Some(TraceEntry::Dropped { count: 1 })
        ));
    }

    #[test]
    fn serializes_entries_with_js_field_names() {
        let span = serde_json::to_value(span_entry(3)).unwrap();
        assert_eq!(span["kind"], "span");
        assert_eq!(span["startUnixNano"], "1775000000000000000");
        assert_eq!(span["endUnixNano"], "1775000000000001000");
        assert!(span.get("start_unix_nano").is_none());

        let log = serde_json::to_value(log_entry(4)).unwrap();
        assert_eq!(log["kind"], "log");
        assert_eq!(log["timestampUnixNano"], "1775000000000002000");
        assert!(log.get("timestamp_unix_nano").is_none());

        let dropped = serde_json::to_value(TraceEntry::Dropped { count: 5 }).unwrap();
        assert_eq!(dropped["kind"], "dropped");
        assert_eq!(dropped["count"], 5);
    }
}
