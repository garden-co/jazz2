use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jazz_tools::{
    AppContext, AppId, ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder, Schema,
    SchemaBuilder, TableSchema, Value,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn row<const N: usize>(pairs: [(&str, Value); N]) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScenarioMode {
    Interactive,
    OfflineReconnect,
    ColdStart,
}

#[derive(Debug, Clone, Deserialize)]
struct ScenarioMixEntry {
    operation: String,
    weight: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct ScenarioConfig {
    id: String,
    name: String,
    seed: u64,
    mode: ScenarioMode,
    operation_count: Option<usize>,
    mix: Option<Vec<ScenarioMixEntry>>,
    offline_write_count: Option<usize>,
    timeout_seconds: Option<u64>,
    reopen_cycles: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct DatasetProfile {
    id: String,
    seed: u64,
    users: usize,
    organizations: usize,
    projects: usize,
    tasks: usize,
    comments: usize,
    watchers_per_task: usize,
    activity_events: usize,
    hot_project_fraction: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskRecord {
    id: ObjectId,
    project_idx: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SeedState {
    users: Vec<ObjectId>,
    projects: Vec<ObjectId>,
    tasks: Vec<TaskRecord>,
    comments_per_task: Vec<usize>,
    hot_project_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct OpSummary {
    count: usize,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
struct BenchResult {
    scenario_id: String,
    scenario_name: String,
    profile_id: String,
    topology: String,
    seed: u64,
    total_operations: usize,
    wall_time_ms: f64,
    throughput_ops_per_sec: f64,
    operation_summaries: BTreeMap<String, OpSummary>,
    extra: serde_json::Value,
}

#[derive(Debug, Clone)]
struct Args {
    scenario_path: PathBuf,
    profile_path: PathBuf,
    server_url: Option<String>,
    app_id: AppId,
    data_dir: Option<PathBuf>,
    seed_state_path: Option<PathBuf>,
    prepare_only: bool,
    reuse_seed: bool,
}

#[derive(Debug, Clone)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        if upper <= 1 {
            return 0;
        }
        (self.next_u64() as usize) % upper
    }

    fn pick_weighted_index(&mut self, weights: &[u32]) -> usize {
        let total: u64 = weights.iter().map(|w| *w as u64).sum();
        if total == 0 {
            return 0;
        }
        let mut cursor = self.next_u64() % total;
        for (idx, &weight) in weights.iter().enumerate() {
            let w = weight as u64;
            if cursor < w {
                return idx;
            }
            cursor -= w;
        }
        weights.len().saturating_sub(1)
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_micros() as u64
}

fn progress(message: impl AsRef<str>) {
    eprintln!("[realistic-progress][native] {}", message.as_ref());
}

fn report_loop_progress(label: &str, index: usize, total: usize) {
    if total == 0 {
        return;
    }
    let step = (total / 4).max(1);
    if index > 0 && index.is_multiple_of(step) {
        progress(format!("{}: {}/{}", label, index, total));
    }
}

fn parse_args() -> Result<Args, DynError> {
    let mut scenario_path: Option<PathBuf> = None;
    let mut profile_path: Option<PathBuf> = None;
    let mut server_url: Option<String> = None;
    let mut app_id_raw: Option<String> = None;
    let mut data_dir: Option<PathBuf> = None;
    let mut seed_state_path: Option<PathBuf> = None;
    let mut prepare_only = false;
    let mut reuse_seed = false;

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--scenario" => {
                let value = it.next().ok_or("--scenario requires a value")?;
                scenario_path = Some(PathBuf::from(value));
            }
            "--profile" => {
                let value = it.next().ok_or("--profile requires a value")?;
                profile_path = Some(PathBuf::from(value));
            }
            "--server-url" => {
                let value = it.next().ok_or("--server-url requires a value")?;
                server_url = Some(value);
            }
            "--app-id" => {
                let value = it.next().ok_or("--app-id requires a value")?;
                app_id_raw = Some(value);
            }
            "--data-dir" => {
                let value = it.next().ok_or("--data-dir requires a value")?;
                data_dir = Some(PathBuf::from(value));
            }
            "--seed-state" => {
                let value = it.next().ok_or("--seed-state requires a value")?;
                seed_state_path = Some(PathBuf::from(value));
            }
            "--prepare-only" => {
                prepare_only = true;
            }
            "--reuse-seed" => {
                reuse_seed = true;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            unknown => {
                return Err(format!("Unknown argument: {}", unknown).into());
            }
        }
    }

    let scenario_path = scenario_path
        .unwrap_or_else(|| PathBuf::from("benchmarks/realistic/scenarios/w1_interactive.json"));
    let profile_path =
        profile_path.unwrap_or_else(|| PathBuf::from("benchmarks/realistic/profiles/s.json"));

    let app_id = if let Some(raw) = app_id_raw {
        AppId::from_string(&raw).unwrap_or_else(|_| AppId::from_name(&raw))
    } else {
        AppId::from_name("realistic-bench")
    };

    Ok(Args {
        scenario_path,
        profile_path,
        server_url,
        app_id,
        data_dir,
        seed_state_path,
        prepare_only,
        reuse_seed,
    })
}

fn print_help() {
    eprintln!(
        "\
realistic_bench

Usage:
  cargo run -p jazz-tools --features client --example realistic_bench -- [options]

Options:
  --scenario <path>    Scenario JSON (default: benchmarks/realistic/scenarios/w1_interactive.json)
  --profile <path>     Profile JSON  (default: benchmarks/realistic/profiles/s.json)
  --server-url <url>   Optional server URL for sync scenarios (required for W3)
  --app-id <id|name>   App ID UUID or deterministic name (default: realistic-bench)
  --data-dir <path>    Persist benchmark files here instead of temp dir
  --seed-state <path>  Persist or reuse serialized seeded dataset metadata
  --prepare-only       Seed data and exit without running the scenario body
  --reuse-seed         Reuse data-dir and --seed-state instead of reseeding
  -h, --help
"
    );
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, DynError> {
    let raw = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), DynError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let raw = serde_json::to_vec_pretty(value)?;
    fs::write(path, raw)?;
    Ok(())
}

fn benchmark_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("display_name", ColumnType::Text)
                .column("email", ColumnType::Text),
        )
        .table(
            TableSchema::builder("organizations")
                .column("name", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp),
        )
        .table(
            TableSchema::builder("memberships")
                .fk_column("organization_id", "organizations")
                .fk_column("user_id", "users")
                .column("role", ColumnType::Text),
        )
        .table(
            TableSchema::builder("projects")
                .fk_column("organization_id", "organizations")
                .column("name", ColumnType::Text)
                .column("archived", ColumnType::Boolean)
                .column("updated_at", ColumnType::Timestamp),
        )
        .table(
            TableSchema::builder("tasks")
                .fk_column("project_id", "projects")
                .column("title", ColumnType::Text)
                .column("status", ColumnType::Text)
                .column("priority", ColumnType::Integer)
                .fk_column("assignee_id", "users")
                .column("updated_at", ColumnType::Timestamp)
                .nullable_column("due_at", ColumnType::Timestamp),
        )
        .table(
            TableSchema::builder("task_comments")
                .fk_column("task_id", "tasks")
                .fk_column("author_id", "users")
                .column("body", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp),
        )
        .table(
            TableSchema::builder("task_watchers")
                .fk_column("task_id", "tasks")
                .fk_column("user_id", "users"),
        )
        .table(
            TableSchema::builder("activity_events")
                .fk_column("project_id", "projects")
                .nullable_fk_column("task_id", "tasks")
                .fk_column("actor_id", "users")
                .column("kind", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp)
                .column("payload", ColumnType::Text),
        )
        .build()
}

async fn connect_client(
    app_id: AppId,
    data_dir: PathBuf,
    server_url: Option<&str>,
) -> Result<JazzClient, DynError> {
    let context = AppContext {
        app_id,
        client_id: None,
        schema: benchmark_schema(),
        server_url: server_url.unwrap_or("").to_string(),
        data_dir,
        storage: jazz_tools::ClientStorage::Persistent,
        jwt_token: None,
        backend_secret: None,
        admin_secret: None,
        sync_tracer: None,
    };
    Ok(JazzClient::connect(context).await?)
}

async fn seed_dataset(
    client: &JazzClient,
    profile: &DatasetProfile,
) -> Result<SeedState, DynError> {
    progress(format!(
        "seeding profile={} users={} orgs={} projects={} tasks={} comments={}",
        profile.id,
        profile.users,
        profile.organizations,
        profile.projects,
        profile.tasks,
        profile.comments
    ));

    let mut users = Vec::with_capacity(profile.users);
    let mut organizations = Vec::with_capacity(profile.organizations);
    let mut projects = Vec::with_capacity(profile.projects);
    let mut tasks = Vec::with_capacity(profile.tasks);
    let mut comments_per_task = vec![0usize; profile.tasks];

    let start_ts = now_micros();

    for i in 0..profile.users {
        report_loop_progress("seed users", i, profile.users);
        let (id, _row_values) = client
            .create(
                "users",
                row([
                    ("display_name", Value::Text(format!("User {}", i))),
                    ("email", Value::Text(format!("user{}@bench.test", i))),
                ]),
            )
            .await?;
        users.push(id);
    }

    for i in 0..profile.organizations {
        report_loop_progress("seed organizations", i, profile.organizations);
        let (id, _row_values) = client
            .create(
                "organizations",
                row([
                    ("name", Value::Text(format!("Org {}", i))),
                    ("created_at", Value::Timestamp(start_ts + i as u64)),
                ]),
            )
            .await?;
        organizations.push(id);
    }

    for i in 0..profile.users {
        report_loop_progress("seed memberships", i, profile.users);
        let org = organizations[i % organizations.len()];
        let user = users[i];
        let _ = client
            .create(
                "memberships",
                row([
                    ("organization_id", Value::Uuid(org)),
                    ("user_id", Value::Uuid(user)),
                    (
                        "role",
                        Value::Text(if i % 9 == 0 { "admin" } else { "member" }.to_string()),
                    ),
                ]),
            )
            .await?;
    }

    for i in 0..profile.projects {
        report_loop_progress("seed projects", i, profile.projects);
        let org = organizations[i % organizations.len()];
        let (id, _row_values) = client
            .create(
                "projects",
                row([
                    ("organization_id", Value::Uuid(org)),
                    ("name", Value::Text(format!("Project {}", i))),
                    ("archived", Value::Boolean(false)),
                    ("updated_at", Value::Timestamp(start_ts + i as u64)),
                ]),
            )
            .await?;
        projects.push(id);
    }

    const STATUSES: [&str; 4] = ["todo", "in_progress", "review", "done"];
    for i in 0..profile.tasks {
        report_loop_progress("seed tasks", i, profile.tasks);
        let project_idx = i % projects.len();
        let assignee_idx = i % users.len();
        let (id, _row_values) = client
            .create(
                "tasks",
                row([
                    ("project_id", Value::Uuid(projects[project_idx])),
                    ("title", Value::Text(format!("Task {}", i))),
                    (
                        "status",
                        Value::Text(STATUSES[i % STATUSES.len()].to_string()),
                    ),
                    ("priority", Value::Integer((1 + (i % 4)) as i32)),
                    ("assignee_id", Value::Uuid(users[assignee_idx])),
                    ("updated_at", Value::Timestamp(start_ts + i as u64)),
                    ("due_at", Value::Timestamp(start_ts + (i as u64 * 7))),
                ]),
            )
            .await?;
        tasks.push(TaskRecord { id, project_idx });
    }

    for i in 0..profile.comments {
        report_loop_progress("seed comments", i, profile.comments);
        let task_idx = i % tasks.len();
        let author = users[(i * 7) % users.len()];
        let _ = client
            .create(
                "task_comments",
                row([
                    ("task_id", Value::Uuid(tasks[task_idx].id)),
                    ("author_id", Value::Uuid(author)),
                    ("body", Value::Text(format!("Comment {} body", i))),
                    ("created_at", Value::Timestamp(start_ts + i as u64)),
                ]),
            )
            .await?;
        comments_per_task[task_idx] += 1;
    }

    for (task_idx, task) in tasks.iter().enumerate() {
        report_loop_progress("seed task_watchers", task_idx, tasks.len());
        for w in 0..profile.watchers_per_task {
            let watcher = users[(task_idx + w) % users.len()];
            let _ = client
                .create(
                    "task_watchers",
                    row([
                        ("task_id", Value::Uuid(task.id)),
                        ("user_id", Value::Uuid(watcher)),
                    ]),
                )
                .await?;
        }
    }

    for i in 0..profile.activity_events {
        report_loop_progress("seed activity_events", i, profile.activity_events);
        let task_idx = i % tasks.len();
        let task = &tasks[task_idx];
        let actor = users[(i * 11) % users.len()];
        let _ = client
            .create(
                "activity_events",
                row([
                    ("project_id", Value::Uuid(projects[task.project_idx])),
                    ("task_id", Value::Uuid(task.id)),
                    ("actor_id", Value::Uuid(actor)),
                    (
                        "kind",
                        Value::Text(
                            if i % 3 == 0 {
                                "task_updated"
                            } else {
                                "comment_added"
                            }
                            .into(),
                        ),
                    ),
                    ("created_at", Value::Timestamp(start_ts + i as u64)),
                    ("payload", Value::Text(format!("{{\"event\":{}}}", i))),
                ]),
            )
            .await?;
    }

    let hot_project_count = ((projects.len() as f64 * profile.hot_project_fraction)
        .round()
        .clamp(1.0, projects.len() as f64)) as usize;

    progress("seeding complete");

    Ok(SeedState {
        users,
        projects,
        tasks,
        comments_per_task,
        hot_project_count,
    })
}

fn summarize_op(latencies_ms: &[f64]) -> OpSummary {
    if latencies_ms.is_empty() {
        return OpSummary {
            count: 0,
            avg_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
        };
    }

    let mut sorted = latencies_ms.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let at = |p: f64| -> f64 {
        let idx = ((sorted.len() as f64 * p).ceil() as usize)
            .saturating_sub(1)
            .min(sorted.len() - 1);
        sorted[idx]
    };
    let avg_ms = sorted.iter().sum::<f64>() / sorted.len() as f64;
    OpSummary {
        count: sorted.len(),
        avg_ms,
        p50_ms: at(0.50),
        p95_ms: at(0.95),
        p99_ms: at(0.99),
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), DynError> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else if file_type.is_file() {
            fs::copy(&src_path, &dst_path)?;
            fs::set_permissions(&dst_path, fs::metadata(&src_path)?.permissions())?;
        }
    }
    Ok(())
}

fn choose_status(rng: &mut Lcg) -> &'static str {
    match rng.next_usize(4) {
        0 => "todo",
        1 => "in_progress",
        2 => "review",
        _ => "done",
    }
}

async fn run_w1_interactive(
    client: &JazzClient,
    scenario: &ScenarioConfig,
    profile: &DatasetProfile,
    seed: &mut SeedState,
    topology: &str,
) -> Result<BenchResult, DynError> {
    let mix = scenario
        .mix
        .as_ref()
        .ok_or("W1 requires scenario.mix entries")?;
    if mix.is_empty() {
        return Err("W1 requires at least one operation in mix".into());
    }
    let operation_count = scenario.operation_count.unwrap_or(1000);
    let mut rng = Lcg::new(scenario.seed ^ profile.seed);

    let weights: Vec<u32> = mix.iter().map(|m| m.weight).collect();
    let mut latencies: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let wall_start = Instant::now();
    progress(format!("W1 start operations={}", operation_count));

    for i in 0..operation_count {
        let step = (operation_count / 10).max(1);
        if i > 0 && i % step == 0 {
            progress(format!("W1 progress {}/{}", i, operation_count));
        }
        let mix_idx = rng.pick_weighted_index(&weights);
        let op = &mix[mix_idx].operation;
        let t0 = Instant::now();

        match op.as_str() {
            "query_board" => {
                let project_idx = rng.next_usize(seed.hot_project_count);
                let query = QueryBuilder::new("tasks")
                    .filter_eq("project_id", Value::Uuid(seed.projects[project_idx]))
                    .order_by_desc("updated_at")
                    .limit(200)
                    .build();
                let _ = client.query(query, None).await?;
            }
            "query_my_work" => {
                let user_idx = rng.next_usize(seed.users.len());
                let query = QueryBuilder::new("tasks")
                    .filter_eq("assignee_id", Value::Uuid(seed.users[user_idx]))
                    .filter_eq("status", Value::Text("in_progress".into()))
                    .order_by_desc("updated_at")
                    .limit(200)
                    .build();
                let _ = client.query(query, None).await?;
            }
            "query_task_detail" => {
                let task_idx = rng.next_usize(seed.tasks.len());
                let task = &seed.tasks[task_idx];

                let comments = QueryBuilder::new("task_comments")
                    .filter_eq("task_id", Value::Uuid(task.id))
                    .order_by_desc("created_at")
                    .limit(200)
                    .build();
                let _ = client.query(comments, None).await?;

                let activity = QueryBuilder::new("activity_events")
                    .filter_eq("task_id", Value::Uuid(task.id))
                    .order_by_desc("created_at")
                    .limit(200)
                    .build();
                let _ = client.query(activity, None).await?;
            }
            "update_task_status" => {
                let task_idx = rng.next_usize(seed.tasks.len());
                let task = &seed.tasks[task_idx];
                let assignee = seed.users[rng.next_usize(seed.users.len())];
                client
                    .update(
                        task.id,
                        vec![
                            (
                                "status".to_string(),
                                Value::Text(choose_status(&mut rng).to_string()),
                            ),
                            (
                                "priority".to_string(),
                                Value::Integer((1 + rng.next_usize(4)) as i32),
                            ),
                            ("assignee_id".to_string(), Value::Uuid(assignee)),
                            ("updated_at".to_string(), Value::Timestamp(now_micros())),
                        ],
                    )
                    .await?;
            }
            "insert_comment" => {
                let task_idx = rng.next_usize(seed.tasks.len());
                let author = seed.users[rng.next_usize(seed.users.len())];
                client
                    .create(
                        "task_comments",
                        row([
                            ("task_id", Value::Uuid(seed.tasks[task_idx].id)),
                            ("author_id", Value::Uuid(author)),
                            ("body", Value::Text(format!("interactive comment {}", i))),
                            ("created_at", Value::Timestamp(now_micros())),
                        ]),
                    )
                    .await?;
                seed.comments_per_task[task_idx] += 1;
            }
            "update_project_meta" => {
                let project_idx = rng.next_usize(seed.projects.len());
                client
                    .update(
                        seed.projects[project_idx],
                        vec![
                            (
                                "name".to_string(),
                                Value::Text(format!("Project {} v{}", project_idx, i)),
                            ),
                            ("updated_at".to_string(), Value::Timestamp(now_micros())),
                        ],
                    )
                    .await?;
            }
            unknown => {
                return Err(format!("Unknown W1 operation '{}'", unknown).into());
            }
        }

        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
        latencies.entry(op.clone()).or_default().push(elapsed_ms);
    }

    let wall_time_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let throughput = if wall_time_ms > 0.0 {
        operation_count as f64 / (wall_time_ms / 1000.0)
    } else {
        0.0
    };
    let operation_summaries = latencies
        .iter()
        .map(|(k, v)| (k.clone(), summarize_op(v)))
        .collect::<BTreeMap<_, _>>();

    Ok(BenchResult {
        scenario_id: scenario.id.clone(),
        scenario_name: scenario.name.clone(),
        profile_id: profile.id.clone(),
        topology: topology.to_string(),
        seed: scenario.seed ^ profile.seed,
        total_operations: operation_count,
        wall_time_ms,
        throughput_ops_per_sec: throughput,
        operation_summaries,
        extra: json!({
            "hot_projects": seed.hot_project_count,
            "dataset": {
                "users": profile.users,
                "projects": profile.projects,
                "tasks": profile.tasks,
                "comments": profile.comments
            }
        }),
    })
}

async fn run_w3_offline_reconnect(
    app_id: AppId,
    data_dir: PathBuf,
    scenario: &ScenarioConfig,
    profile: &DatasetProfile,
    server_url: &str,
) -> Result<BenchResult, DynError> {
    let offline_writes = scenario.offline_write_count.unwrap_or(200);
    let timeout_secs = scenario.timeout_seconds.unwrap_or(45);
    let mut rng = Lcg::new(scenario.seed ^ profile.seed);

    let offline_client = connect_client(app_id, data_dir.clone(), None).await?;
    let mut seed = seed_dataset(&offline_client, profile).await?;
    let target_task_idx = rng.next_usize(seed.tasks.len());
    let target_task_id = seed.tasks[target_task_idx].id;
    let baseline_comments = seed.comments_per_task[target_task_idx];

    let t_offline_start = Instant::now();
    for i in 0..offline_writes {
        let author = seed.users[rng.next_usize(seed.users.len())];
        let _ = offline_client
            .create(
                "task_comments",
                row([
                    ("task_id", Value::Uuid(target_task_id)),
                    ("author_id", Value::Uuid(author)),
                    (
                        "body",
                        Value::Text(format!("offline_reconnect_marker_{}", i)),
                    ),
                    ("created_at", Value::Timestamp(now_micros())),
                ]),
            )
            .await?;
        seed.comments_per_task[target_task_idx] += 1;
    }
    let offline_write_ms = t_offline_start.elapsed().as_secs_f64() * 1000.0;
    progress("W3 offline writes complete, reconnecting");
    offline_client.shutdown().await?;

    let reconnect_start = Instant::now();
    let online_client = connect_client(app_id, data_dir.clone(), Some(server_url)).await?;

    let target_count = baseline_comments + offline_writes;
    let timeout_at = Instant::now() + Duration::from_secs(timeout_secs);
    let mut observed_count: usize;
    let mut polls = 0usize;

    loop {
        let query = QueryBuilder::new("task_comments")
            .filter_eq("task_id", Value::Uuid(target_task_id))
            .build();
        let rows = online_client
            .query(query, Some(DurabilityTier::EdgeServer))
            .await?;
        observed_count = rows.len();
        polls += 1;
        if polls.is_multiple_of(10) {
            progress(format!(
                "W3 polling observed={} target={} polls={}",
                observed_count, target_count, polls
            ));
        }

        if observed_count >= target_count {
            break;
        }
        if Instant::now() >= timeout_at {
            online_client.shutdown().await?;
            return Err(format!(
                "Timed out waiting for edge settlement: observed={}, expected_at_least={}",
                observed_count, target_count
            )
            .into());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let reconnect_ms = reconnect_start.elapsed().as_secs_f64() * 1000.0;
    progress("W3 reconnect settled");
    online_client.shutdown().await?;

    let mut operation_summaries = BTreeMap::new();
    operation_summaries.insert(
        "offline_writes".to_string(),
        OpSummary {
            count: offline_writes,
            avg_ms: offline_write_ms / offline_writes.max(1) as f64,
            p50_ms: offline_write_ms / offline_writes.max(1) as f64,
            p95_ms: offline_write_ms / offline_writes.max(1) as f64,
            p99_ms: offline_write_ms / offline_writes.max(1) as f64,
        },
    );
    operation_summaries.insert(
        "reconnect_settlement".to_string(),
        OpSummary {
            count: 1,
            avg_ms: reconnect_ms,
            p50_ms: reconnect_ms,
            p95_ms: reconnect_ms,
            p99_ms: reconnect_ms,
        },
    );

    Ok(BenchResult {
        scenario_id: scenario.id.clone(),
        scenario_name: scenario.name.clone(),
        profile_id: profile.id.clone(),
        topology: "single_hop".to_string(),
        seed: scenario.seed ^ profile.seed,
        total_operations: offline_writes + 1,
        wall_time_ms: offline_write_ms + reconnect_ms,
        throughput_ops_per_sec: (offline_writes + 1) as f64
            / ((offline_write_ms + reconnect_ms) / 1000.0).max(0.001),
        operation_summaries,
        extra: json!({
            "target_task_id": target_task_id.to_string(),
            "baseline_comments": baseline_comments,
            "target_comments_after_reconnect": target_count,
            "observed_comments_after_reconnect": observed_count,
            "poll_iterations": polls
        }),
    })
}

async fn run_w4_cold_start(
    app_id: AppId,
    data_dir: PathBuf,
    scenario: &ScenarioConfig,
    profile: &DatasetProfile,
    seed: &SeedState,
) -> Result<BenchResult, DynError> {
    let cycles = scenario.reopen_cycles.unwrap_or(20);
    let hot_project = seed.projects[0];

    let mut latencies = Vec::with_capacity(cycles);
    progress(format!("W4 start cycles={}", cycles));

    for i in 0..cycles {
        report_loop_progress("W4 cycles", i, cycles);
        let cycle_temp = tempfile::tempdir()?;
        let cycle_data_dir = cycle_temp.path().join("db");
        copy_dir_recursive(&data_dir, &cycle_data_dir)?;
        let t0 = Instant::now();
        let client = connect_client(app_id, cycle_data_dir, None).await?;
        let query = QueryBuilder::new("tasks")
            .filter_eq("project_id", Value::Uuid(hot_project))
            .order_by_desc("updated_at")
            .limit(200)
            .build();
        let _ = client.query(query, None).await?;
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
        latencies.push(elapsed_ms);
        client.shutdown().await?;
        drop(cycle_temp);
    }

    let wall_time_ms = latencies.iter().sum::<f64>();
    let mut operation_summaries = BTreeMap::new();
    operation_summaries.insert("cold_reopen".to_string(), summarize_op(&latencies));

    Ok(BenchResult {
        scenario_id: scenario.id.clone(),
        scenario_name: scenario.name.clone(),
        profile_id: profile.id.clone(),
        topology: "local_only".to_string(),
        seed: scenario.seed ^ profile.seed,
        total_operations: cycles,
        wall_time_ms,
        throughput_ops_per_sec: cycles as f64 / (wall_time_ms / 1000.0).max(0.001),
        operation_summaries,
        extra: json!({
            "cycles": cycles,
            "hot_project_id": hot_project.to_string()
        }),
    })
}

fn load_seed_state(path: &Path) -> Result<SeedState, DynError> {
    load_json(path)
}

fn maybe_write_seed_state(path: Option<&Path>, seed: &SeedState) -> Result<(), DynError> {
    if let Some(path) = path {
        write_json(path, seed)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let args = parse_args()?;
    let scenario: ScenarioConfig = load_json(&args.scenario_path)?;
    let profile: DatasetProfile = load_json(&args.profile_path)?;
    progress(format!(
        "start scenario={} mode={:?} profile={}",
        scenario.id, scenario.mode, profile.id
    ));

    let owned_temp: Option<TempDir>;
    let data_dir = if let Some(path) = args.data_dir.clone() {
        fs::create_dir_all(&path)?;
        owned_temp = None;
        path
    } else {
        let temp = tempfile::tempdir()?;
        let path = temp.path().to_path_buf();
        owned_temp = Some(temp);
        path
    };

    let result = match scenario.mode {
        ScenarioMode::Interactive => {
            let topology = if args.server_url.is_some() {
                "single_hop"
            } else {
                "local_only"
            };
            let client =
                connect_client(args.app_id, data_dir.clone(), args.server_url.as_deref()).await?;
            let seed_state_path = args.seed_state_path.as_deref();
            let mut seed = if args.reuse_seed {
                let path = seed_state_path.ok_or("--reuse-seed requires --seed-state")?;
                load_seed_state(path)?
            } else {
                let seed = seed_dataset(&client, &profile).await?;
                maybe_write_seed_state(seed_state_path, &seed)?;
                seed
            };
            if args.prepare_only {
                client.shutdown().await?;
                progress("prepare-only complete");
                drop(owned_temp);
                return Ok(());
            }
            let result =
                run_w1_interactive(&client, &scenario, &profile, &mut seed, topology).await?;
            client.shutdown().await?;
            result
        }
        ScenarioMode::OfflineReconnect => {
            let url = args
                .server_url
                .as_deref()
                .ok_or("W3 requires --server-url (example: --server-url http://127.0.0.1:1625)")?;
            run_w3_offline_reconnect(args.app_id, data_dir.clone(), &scenario, &profile, url)
                .await?
        }
        ScenarioMode::ColdStart => {
            let seed_state_path = args.seed_state_path.as_deref();
            let seed = if args.reuse_seed {
                let path = seed_state_path.ok_or("--reuse-seed requires --seed-state")?;
                load_seed_state(path)?
            } else {
                let seeding_client = connect_client(args.app_id, data_dir.clone(), None).await?;
                let seed = seed_dataset(&seeding_client, &profile).await?;
                maybe_write_seed_state(seed_state_path, &seed)?;
                seeding_client.shutdown().await?;
                seed
            };
            if args.prepare_only {
                progress("prepare-only complete");
                drop(owned_temp);
                return Ok(());
            }
            run_w4_cold_start(args.app_id, data_dir.clone(), &scenario, &profile, &seed).await?
        }
    };

    let _keep_temp_alive = owned_temp;
    progress("benchmark complete; emitting JSON report");
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
