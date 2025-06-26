/* SPDX-License-Identifier: MPL-2.0 */

.text

.global context_switch
.type context_switch, @function
context_switch: # (nxt: *mut TaskContext, cur: *TaskContext)
  b context_switch
.size context_switch, .-context_switch

.global first_context_switch
.type first_context_switch, @function
first_context_switch: # (nxt: *const TaskContext)
  b first_context_switch
.size first_context_switch, .-first_context_switch

.global kernel_task_entry_wrapper
.type kernel_task_entry_wrapper, @function
kernel_task_entry_wrapper:
  b kernel_task_entry_wrapper
.size kernel_task_entry_wrapper, .-kernel_task_entry_wrapper
