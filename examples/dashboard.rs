#![deny(unsafe_code)]

#[cfg(not(feature = "render-tui"))]
compile_error!(
    "The `render-tui` feature must be set, along with either `render-tui-crossterm` or `render-tui-termion`"
);
#[cfg(not(any(feature = "render-tui-crossterm")))]
compile_error!("Please set the 'render-tui-crossterm' feature when using the 'render-tui'");

fn main() -> Result {
    env_logger::init();

    let args: args::Options = argh::from_env();
    futures_lite::future::block_on(work_forever(args))
}

async fn work_forever(mut args: args::Options) -> Result {
    let progress: Arc<_> = prodash::tree::root::Options {
        message_buffer_capacity: args.message_scrollback_buffer_size,
        ..Default::default()
    }
    .create()
    .into();
    {
        let mut sp = progress.add_child("preparation");
        sp.info("warming up");
        spawn(async move {
            async_io::Timer::after(Duration::from_millis(500)).await;
            sp.fail("engine failure");
            async_io::Timer::after(Duration::from_millis(750)).await;
            sp.done("warmup complete");
        })
        .detach();
    }
    // Now we should handle signals to be able to cleanup properly
    let speed = args.speed_multitplier;
    let changing_names = args.changing_names;

    let renderer = args.renderer.take().unwrap_or_else(|| "tui".into());
    let work_min = args.pooled_work_min;
    let work_max = args.pooled_work_max;
    let mut gui_handle = if renderer == "log" {
        let never_ending = spawn(futures_lite::future::pending::<()>());
        Some(never_ending.boxed())
    } else {
        Some(
            shared::launch_ambient_gui(progress.clone(), &renderer, args, false)
                .unwrap()
                .boxed(),
        )
    };

    loop {
        let local_work = new_chunk_of_work(
            NestingLevel(rng().random_range(0..=Key::max_level())),
            progress.clone(),
            speed,
            changing_names,
        )
        .boxed_local();
        let num_chunks = if work_min < work_max {
            rng().random_range(work_min..=work_max)
        } else {
            work_min
        };
        let pooled_work = (0..num_chunks).map(|_| {
            spawn(new_chunk_of_work(
                NestingLevel(rng().random_range(0..=Key::max_level())),
                progress.clone(),
                speed,
                changing_names,
            ))
            .boxed_local()
        });

        match futures_util::future::select(
            join_all(std::iter::once(local_work).chain(pooled_work)),
            gui_handle.take().expect("gui handle"),
        )
        .await
        {
            Either::Left((_workblock_result, running_gui)) => {
                gui_handle = Some(running_gui);
                continue;
            }
            Either::Right(_gui_shutdown) => break,
        }
    }

    if let Some(gui) = gui_handle {
        // gui.cancel();
        gui.await;
    }
    Ok(())
}

async fn work_item(mut progress: Item, speed: f32, changing_names: bool) {
    let max: u8 = rng().random_range(25..=125);
    progress.init(
        if max > WORK_STEPS_NEEDED_FOR_UNBOUNDED_TASK {
            None
        } else {
            Some(max.into())
        },
        if (max as usize % UNITS.len() + 1) == 0 {
            None
        } else {
            UNITS.choose(&mut rng()).copied().map(Into::into)
        },
    );

    for step in 0..max {
        progress.set(step as Step);
        let delay_ms = if rng().random_bool(CHANCE_TO_BLOCK_PER_STEP) {
            let eta = if rng().random_bool(CHANCE_TO_SHOW_ETA) {
                Some(SystemTime::now().add(Duration::from_millis(LONG_WORK_DELAY_MS)))
            } else {
                None
            };
            if rng().random_bool(0.5) {
                progress.halted(REASONS.choose(&mut rng()).unwrap(), eta);
            } else {
                progress.blocked(REASONS.choose(&mut rng()).unwrap(), eta);
            }
            rng().random_range(WORK_DELAY_MS..=LONG_WORK_DELAY_MS)
        } else {
            rng().random_range(SHORT_DELAY_MS..=WORK_DELAY_MS)
        };
        if rng().random_bool(0.01) {
            progress.init(Some(max.into()), UNITS.choose(&mut rng()).copied().map(Into::into))
        }
        if rng().random_bool(0.01) {
            progress.info(*INFO_MESSAGES.choose(&mut rng()).unwrap());
        }
        if rng().random_bool(if changing_names { 0.5 } else { 0.01 }) {
            progress.set_name(WORK_NAMES.choose(&mut rng()).unwrap().to_string());
        }
        async_io::Timer::after(Duration::from_millis((delay_ms as f32 / speed) as u64)).await;
    }
    if rng().random_bool(0.95) {
        progress.done(*DONE_MESSAGES.choose(&mut rng()).unwrap());
    } else {
        progress.fail(*FAIL_MESSAGES.choose(&mut rng()).unwrap());
    }
}

async fn new_chunk_of_work(max: NestingLevel, tree: Arc<Tree>, speed: f32, changing_names: bool) -> Result {
    let NestingLevel(max_level) = max;
    let mut progresses = Vec::new();
    let mut level_progress = tree.add_child(format!("level {} of {}", 1, max_level));
    let mut handles = Vec::new();

    for level in 0..max_level {
        // one-off ambient tasks
        let num_tasks = max_level as usize * 2;
        for id in 0..num_tasks {
            let handle = spawn(work_item(
                level_progress.add_child(format!("{} {}", WORK_NAMES.choose(&mut rng()).unwrap(), id + 1)),
                speed,
                changing_names,
            ));
            handles.push(handle);

            async_io::Timer::after(Duration::from_millis((SPAWN_DELAY_MS as f32 / speed) as u64)).await;
        }
        if level + 1 != max_level {
            let tmp = level_progress.add_child(format!("Level {}", level + 1));
            progresses.push(level_progress);
            level_progress = tmp;
        }
    }

    progresses.push(level_progress);
    for handle in handles.into_iter() {
        handle.await;
    }

    Ok(())
}

struct NestingLevel(u8);
type Result = std::result::Result<(), Box<dyn Error + Send>>;

use std::{
    error::Error,
    ops::Add,
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures_util::{
    FutureExt,
    future::{Either, join_all},
};
use prodash::{
    progress::{Key, Step},
    tree::{Item, Root as Tree},
};
use rand::{RngExt, prelude::IndexedRandom, rng};

const WORK_STEPS_NEEDED_FOR_UNBOUNDED_TASK: u8 = 100;
const UNITS: &[&str] = &["Mb", "kb", "items", "files"];
const REASONS: &[&str] = &["due to star alignment", "IO takes time", "仪表板演示", "just because"];
const WORK_NAMES: &[&str] = &[
    "Downloading Crate",
    "下载板条箱",
    "Running 'cargo geiger'",
    "运行程序 'cargo geiger'",
    "Counting lines of code",
    "计数代码行",
    "Checking for unused dependencies",
    "检查未使用的依赖项",
    "Checking for crate-bloat",
    "检查板条箱膨胀",
    "Generating report",
    "生成报告",
];
const DONE_MESSAGES: &[&str] = &[
    "Yeeeehaa! Finally!!",
    "呀！ 最后！",
    "It feels good to be done!",
    "感觉好极了！",
    "Told you so!!",
    "告诉过你了！",
];
const FAIL_MESSAGES: &[&str] = &[
    "That didn't seem to work!",
    "那似乎没有用！",
    "Oh my… I failed you 😞",
    "哦，我…我让你失败😞",
    "This didn't end well…",
    "结局不好…",
];
const INFO_MESSAGES: &[&str] = &[
    "Making good progress!",
    "进展良好！",
    "Humming along…",
    "嗡嗡作响…",
    "It will be done soooooon…",
    "会很快完成的……",
];
const SHORT_DELAY_MS: u64 = 50;
const WORK_DELAY_MS: u64 = 100;
const LONG_WORK_DELAY_MS: u64 = 2000;
const SPAWN_DELAY_MS: u64 = 200;
const CHANCE_TO_BLOCK_PER_STEP: f64 = 1.0 / 100.0;
const CHANCE_TO_SHOW_ETA: f64 = 0.5;

mod shared;
use shared::{args, spawn};
