// SPDX-License-Identifier: MPL-2.0

//! Scheduling subsystem (in-OSTD part).
//!
//! This module defines what OSTD expects from a scheduling implementation
//! and provides useful functions for controlling the execution flow.

mod fifo_scheduler;
pub mod info;

use spin::Once;

use super::{preempt::cpu_local, processor, Task};
use crate::{cpu::PinCurrentCpu, prelude::*, task::disable_preempt, timer};

/// Injects a scheduler implementation into framework.
///
/// This function can only be called once and must be called during the initialization of kernel.
pub fn inject_scheduler(scheduler: &'static dyn Scheduler<Task>) {
    SCHEDULER.call_once(|| scheduler);

    timer::register_callback(|| {
        SCHEDULER.get().unwrap().local_mut_rq_with(&mut |local_rq| {
            if local_rq.update_current(UpdateFlags::Tick) {
                cpu_local::set_need_preempt();
            }
        })
    });
}

static SCHEDULER: Once<&'static dyn Scheduler<Task>> = Once::new();

/// A per-CPU task scheduler.
pub trait Scheduler<T = Task>: Sync + Send {
    /// Enqueues a runnable task.
    ///
    /// Scheduler developers can perform load-balancing or some accounting work here.
    ///
    /// If the `current` of a CPU needs to be preempted, this method returns the id of
    /// that CPU.
    fn enqueue(&self, runnable: Arc<T>, flags: EnqueueFlags) -> Option<u32>;

    /// Gets an immutable access to the local runqueue of the current CPU core.
    fn local_rq_with(&self, f: &mut dyn FnMut(&dyn LocalRunQueue<T>));

    /// Gets a mutable access to the local runqueue of the current CPU core.
    fn local_mut_rq_with(&self, f: &mut dyn FnMut(&mut dyn LocalRunQueue<T>));
}

/// The state of the current task at the time of scheduling.
#[derive(PartialEq, Copy, Clone)]
pub enum CurrentState {
    /// The current task is still runnable, so it will be kept in the runqueue.
    Runnable,
    /// The current task needs to go to sleep, so it will leave the runqueue.
    NeedSleep,
}

/// The _local_ view of a per-CPU runqueue.
///
/// This local view provides the interface for the runqueue of a CPU core
/// to be inspected and manipulated by the code running on this particular CPU core.
///
/// Conceptually, a local runqueue consists of two parts:
/// (1) a priority queue of runnable tasks;
/// (2) the current running task.
/// The exact definition of "priority" is left for the concrete implementation to decide.
pub trait LocalRunQueue<T = Task> {
    /// Gets the current runnable task.
    fn current(&self) -> Option<&Arc<T>>;

    /// Updates the current runnable task's scheduling statistics and potentially its
    /// position in the queue.
    ///
    /// If the current runnable task needs to be preempted, the method returns `true`.
    fn update_current(&mut self, flags: UpdateFlags) -> bool;

    /// Picks the next runnable task.
    ///
    /// This method returns the selected next runnable task. This task will replace the current
    /// runnable task after the method returns. If there is no candidate for such a task, this
    /// method will do nothing but return `None`.
    ///
    /// After the next runnable task is selected, `current_state` will decide whether or not to
    /// keep the current runnable task in the runqueue. See [`CurrentState`] for details.
    fn pick_next(&mut self, current_state: CurrentState) -> Option<&Arc<T>>;
}

/// Possible triggers of an `enqueue` action.
#[derive(PartialEq, Copy, Clone)]
pub enum EnqueueFlags {
    /// Spawn a new task.
    Spawn,
    /// Wake a sleeping task.
    Wake,
}

/// Possible triggers of an `update_current` action.
#[derive(PartialEq, Copy, Clone)]
pub enum UpdateFlags {
    /// Timer interrupt.
    Tick,
    /// Task waiting.
    Wait,
    /// Task yielding.
    Yield,
}

/// Preempts the current task.
pub(crate) fn might_preempt() {
    if !cpu_local::should_preempt() {
        return;
    }
    yield_now();
}

/// Blocks the current task unless `has_woken()` returns `true`.
///
/// Note that this method may return due to spurious wake events. It's the caller's responsibility
/// to detect them (if necessary).
pub(crate) fn park_current<F>(mut has_woken: F)
where
    F: FnMut() -> bool,
{
    reschedule(|local_rq: &mut dyn LocalRunQueue| {
        if has_woken() {
            return ReschedAction::DoNothing;
        }

        // Note the race conditions: the current task may be woken after the above `has_woken`
        // check, but before the below `pick_next` action, we need to make sure that the wakeup
        // event isn't lost.
        //
        // Currently, for the FIFO scheduler, `Scheduler::enqueue` will try to lock `local_rq` when
        // the above race condition occurs, so it will wait until we finish calling the `pick_next`
        // method and nothing bad will happen. This may need to be revisited after more complex
        // schedulers are introduced.

        local_rq.update_current(UpdateFlags::Wait);
        if let Some(next_task) = local_rq.pick_next(CurrentState::NeedSleep) {
            return ReschedAction::SwitchTo(next_task.clone());
        }

        ReschedAction::Retry
    });
}

/// Unblocks a target task.
pub(crate) fn unpark_target(runnable: Arc<Task>) {
    let target_cpu = SCHEDULER
        .get()
        .unwrap()
        .enqueue(runnable, EnqueueFlags::Wake);
    if let Some(target_cpu_id) = target_cpu {
        set_need_preempt(target_cpu_id);
    }
}

/// Enqueues a newly built task.
///
/// Note that the new task is not guaranteed to run at once.
pub(super) fn run_new_task(runnable: Arc<Task>) {
    // FIXME: remove this check for `SCHEDULER`.
    // Currently OSTD cannot know whether its user has injected a scheduler.
    if !SCHEDULER.is_completed() {
        fifo_scheduler::init();
    }

    let target_cpu = SCHEDULER
        .get()
        .unwrap()
        .enqueue(runnable, EnqueueFlags::Spawn);
    if let Some(target_cpu_id) = target_cpu {
        set_need_preempt(target_cpu_id);
    }

    might_preempt();
}

fn set_need_preempt(cpu_id: u32) {
    let preempt_guard = disable_preempt();

    if preempt_guard.current_cpu() == cpu_id {
        cpu_local::set_need_preempt();
    } else {
        // TODO: Send IPIs to set remote CPU's `need_preempt`
    }
}

/// Dequeues the current task from its runqueue.
///
/// This should only be called if the current is to exit.
pub(super) fn exit_current() {
    reschedule(|local_rq: &mut dyn LocalRunQueue| {
        if let Some(next_task) = local_rq.pick_next(CurrentState::NeedSleep) {
            ReschedAction::SwitchTo(next_task.clone())
        } else {
            ReschedAction::Retry
        }
    })
}

/// Yields execution.
pub(super) fn yield_now() {
    reschedule(|local_rq| {
        local_rq.update_current(UpdateFlags::Yield);
        if let Some(next_task) = local_rq.pick_next(CurrentState::Runnable) {
            ReschedAction::SwitchTo(next_task.clone())
        } else {
            ReschedAction::DoNothing
        }
    })
}

/// Do rescheduling by acting on the scheduling decision (`ReschedAction`) made by a
/// user-given closure.
///
/// The closure makes the scheduling decision by taking the local runqueue has its input.
fn reschedule<F>(mut f: F)
where
    F: FnMut(&mut dyn LocalRunQueue) -> ReschedAction,
{
    let next_task = loop {
        let mut action = ReschedAction::DoNothing;
        SCHEDULER.get().unwrap().local_mut_rq_with(&mut |rq| {
            action = f(rq);
        });

        match action {
            ReschedAction::DoNothing => {
                return;
            }
            ReschedAction::Retry => {
                continue;
            }
            ReschedAction::SwitchTo(next_task) => {
                break next_task;
            }
        };
    };

    // FIXME: At this point, we need to prevent the current task from being scheduled on another
    // CPU core. However, we currently have no way to ensure this. This is a soundness hole and
    // should be fixed. See <https://github.com/asterinas/asterinas/issues/1471> for details.

    cpu_local::clear_need_preempt();
    processor::switch_to_task(next_task);
}

/// Possible actions of a rescheduling.
enum ReschedAction {
    /// Keep running current task and do nothing.
    DoNothing,
    /// Loop until finding a task to swap out the current.
    Retry,
    /// Switch to target task.
    SwitchTo(Arc<Task>),
}
