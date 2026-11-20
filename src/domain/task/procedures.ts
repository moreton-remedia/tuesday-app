import { ORPCError } from '@orpc/client';
import { and, asc, eq, inArray, sql } from 'drizzle-orm';
import { z } from 'zod';
import { database, workspaceScoped } from '@/rpc/base';
import { taskTable, subitemTable, groupTable, automationTable, columnTable, boardTable } from '@/database/schema';
import { nanoid } from '@/lib/utils';
import type { AutomationAction, AutomationTrigger } from '@/database/schema';
import { broadcastBoardUpdate } from '@/lib/board-events';

const fieldsSchema = z.record(z.string(), z.string());

async function boardIdFromGroup(groupId: string): Promise<string | null> {
  const [row] = await database.select({ boardId: groupTable.boardId })
    .from(groupTable).where(eq(groupTable.id, groupId));
  return row?.boardId ?? null;
}

async function boardIdFromTask(taskId: string): Promise<string | null> {
  const [row] = await database.select({ boardId: groupTable.boardId })
    .from(taskTable)
    .innerJoin(groupTable, eq(taskTable.groupId, groupTable.id))
    .where(eq(taskTable.id, taskId));
  return row?.boardId ?? null;
}

async function boardIdFromSubitem(subitemId: string): Promise<string | null> {
  const [row] = await database.select({ boardId: groupTable.boardId })
    .from(subitemTable)
    .innerJoin(taskTable, eq(subitemTable.taskId, taskTable.id))
    .innerJoin(groupTable, eq(taskTable.groupId, groupTable.id))
    .where(eq(subitemTable.id, subitemId));
  return row?.boardId ?? null;
}

// ── Tasks ─────────────────────────────────────────────────────────────────────

export const createTask = workspaceScoped
  .input(z.object({ groupId: z.string(), name: z.string().default(''), position: z.number().optional() }))
  .handler(async ({ input }) => {
    const [task] = await database.insert(taskTable).values({
      id: nanoid(),
      groupId: input.groupId,
      name: input.name,
      position: input.position ?? Date.now(),
    }).returning();
    boardIdFromGroup(input.groupId).then(id => id && broadcastBoardUpdate(id)).catch(() => {});
    return task;
  });

export const updateTask = workspaceScoped
  .input(z.object({
    taskId: z.string(),
    name: z.string().optional(),
    done: z.boolean().optional(),
    expanded: z.boolean().optional(),
    fields: fieldsSchema.optional(),
    groupId: z.string().optional(), // for moving between groups
  }))
  .handler(async ({ input }) => {
    const patch: Record<string, unknown> = { updatedAt: new Date() };
    if (input.name !== undefined) patch.name = input.name;
    if (input.done !== undefined) patch.done = input.done ? 1 : 0;
    if (input.expanded !== undefined) patch.expanded = input.expanded ? 1 : 0;
    if (input.fields !== undefined) patch.fields = input.fields;
    if (input.groupId !== undefined) patch.groupId = input.groupId;

    // Capture old board before update so we can notify it too when moving cross-board
    let oldBoardId: string | null = null;
    if (input.groupId !== undefined) {
      const [old] = await database.select({ groupId: taskTable.groupId })
        .from(taskTable).where(eq(taskTable.id, input.taskId));
      if (old) oldBoardId = await boardIdFromGroup(old.groupId);
    }

    const [task] = await database.update(taskTable).set(patch).where(eq(taskTable.id, input.taskId)).returning();
    if (!task) throw new ORPCError('NOT_FOUND', { message: 'Task not found.' });

    // When fields change, await automations so their side-effects are committed before
    // the server responds (same reasoning as updateTaskFields above).
    if (input.fields !== undefined) {
      await Promise.all([
        runTaskAutomations(task.id, task.groupId, (task.fields as Record<string, string>) ?? {}).catch(() => {}),
        runAutomationsForLinksToTask(task.id).catch(() => {}),
      ]);
    }

    const newBoardId = await boardIdFromGroup(task.groupId);
    if (newBoardId) broadcastBoardUpdate(newBoardId);
    if (oldBoardId && oldBoardId !== newBoardId) broadcastBoardUpdate(oldBoardId);
    return task;
  });

export const updateTaskFields = workspaceScoped
  .input(z.object({ taskId: z.string(), fields: fieldsSchema }))
  .handler(async ({ input }) => {
    const [task] = await database.update(taskTable)
      .set({ fields: input.fields, updatedAt: new Date() })
      .where(eq(taskTable.id, input.taskId))
      .returning();
    if (!task) throw new ORPCError('NOT_FOUND', { message: 'Task not found.' });
    // Await automations so their side-effects (e.g. a set-status action) are committed
    // before the server responds — the client's invalidation then fetches the fully
    // resolved state in one round trip instead of racing the fire-and-forget.
    await Promise.all([
      runTaskAutomations(task.id, task.groupId, input.fields).catch(() => {}),
      runAutomationsForLinksToTask(task.id).catch(() => {}),
    ]);
    boardIdFromGroup(task.groupId).then(id => id && broadcastBoardUpdate(id)).catch(() => {});
    return task;
  });

export const deleteTask = workspaceScoped
  .input(z.object({ taskId: z.string() }))
  .handler(async ({ input }) => {
    const boardId = await boardIdFromTask(input.taskId);
    await database.delete(taskTable).where(eq(taskTable.id, input.taskId));
    if (boardId) broadcastBoardUpdate(boardId);
    return { success: true };
  });

export const deleteTasks = workspaceScoped
  .input(z.object({ taskIds: z.array(z.string()) }))
  .handler(async ({ input }) => {
    if (!input.taskIds.length) return { success: true };
    const boardId = await boardIdFromTask(input.taskIds[0]!);
    await database.delete(taskTable).where(inArray(taskTable.id, input.taskIds));
    if (boardId) broadcastBoardUpdate(boardId);
    return { success: true };
  });

export const reorderTasks = workspaceScoped
  .input(z.object({ taskIds: z.array(z.string()) }))
  .handler(async ({ input }) => {
    if (!input.taskIds.length) return { success: true };
    await Promise.all(
      input.taskIds.map((id, index) =>
        database.update(taskTable)
          .set({ position: index, updatedAt: new Date() })
          .where(eq(taskTable.id, id)),
      ),
    );
    const boardId = await boardIdFromTask(input.taskIds[0]!);
    if (boardId) broadcastBoardUpdate(boardId);
    return { success: true };
  });

// ── Subitems ──────────────────────────────────────────────────────────────────

export const createSubitem = workspaceScoped
  .input(z.object({ taskId: z.string(), name: z.string().default(''), position: z.number().optional() }))
  .handler(async ({ input }) => {
    const [sub] = await database.insert(subitemTable).values({
      id: nanoid(),
      taskId: input.taskId,
      name: input.name,
      position: input.position ?? Date.now(),
    }).returning();
    boardIdFromTask(input.taskId).then(id => id && broadcastBoardUpdate(id)).catch(() => {});
    return sub;
  });

export const updateSubitem = workspaceScoped
  .input(z.object({
    subitemId: z.string(),
    name: z.string().optional(),
    done: z.boolean().optional(),
    fields: fieldsSchema.optional(),
  }))
  .handler(async ({ input }) => {
    const patch: Record<string, unknown> = { updatedAt: new Date() };
    if (input.name !== undefined) patch.name = input.name;
    if (input.done !== undefined) patch.done = input.done ? 1 : 0;
    if (input.fields !== undefined) patch.fields = input.fields;
    const [sub] = await database.update(subitemTable).set(patch).where(eq(subitemTable.id, input.subitemId)).returning();
    if (!sub) throw new ORPCError('NOT_FOUND', { message: 'Subitem not found.' });

    // Await automations when subitem fields change so side-effects are committed before responding.
    if (input.fields !== undefined) {
      const [parentTask] = await database.select({ groupId: taskTable.groupId })
        .from(taskTable).where(eq(taskTable.id, sub.taskId));
      if (parentTask) {
        await runSubitemTriggeredAutomations(sub.taskId, parentTask.groupId, input.fields).catch(() => {});
      }
    }

    boardIdFromTask(sub.taskId).then(id => id && broadcastBoardUpdate(id)).catch(() => {});
    return sub;
  });

export const deleteSubitem = workspaceScoped
  .input(z.object({ subitemId: z.string() }))
  .handler(async ({ input }) => {
    const boardId = await boardIdFromSubitem(input.subitemId);
    await database.delete(subitemTable).where(eq(subitemTable.id, input.subitemId));
    if (boardId) broadcastBoardUpdate(boardId);
    return { success: true };
  });

export const deleteSubitems = workspaceScoped
  .input(z.object({ subitemIds: z.array(z.string()) }))
  .handler(async ({ input }) => {
    if (!input.subitemIds.length) return { success: true };
    const boardId = await boardIdFromSubitem(input.subitemIds[0]!);
    await database.delete(subitemTable).where(inArray(subitemTable.id, input.subitemIds));
    if (boardId) broadcastBoardUpdate(boardId);
    return { success: true };
  });

export const reorderSubitems = workspaceScoped
  .input(z.object({ subitemIds: z.array(z.string()) }))
  .handler(async ({ input }) => {
    if (!input.subitemIds.length) return { success: true };
    await Promise.all(
      input.subitemIds.map((id, index) =>
        database.update(subitemTable)
          .set({ position: index, updatedAt: new Date() })
          .where(eq(subitemTable.id, id)),
      ),
    );
    const boardId = await boardIdFromSubitem(input.subitemIds[0]!);
    if (boardId) broadcastBoardUpdate(boardId);
    return { success: true };
  });

// ── Automation engine ─────────────────────────────────────────────────────────

/** Evaluate whether a trigger's conditions match the given field snapshot. */
async function evaluateTrigger(
  trigger: AutomationTrigger,
  fields: Record<string, string>,
): Promise<boolean> {
  if (trigger.type === 'date-is') {
    const raw = fields[trigger.colId];
    if (!raw) return false;
    // Compare date portion only (strip optional time component)
    const dateOnly = raw.includes('T') ? raw.slice(0, 10) : raw;
    const today = new Date().toISOString().slice(0, 10);
    if (trigger.value === 'today')   return dateOnly === today;
    if (trigger.value === 'past')    return dateOnly < today;
    if (trigger.value === 'future')  return dateOnly > today;
    return false;
  }

  if (trigger.type === 'link-changes') {
    const linkedTaskId = fields[trigger.colId];
    if (!linkedTaskId || !trigger.linkedColId) return false;
    const [linkedTask] = await database.select({ fields: taskTable.fields })
      .from(taskTable).where(eq(taskTable.id, linkedTaskId));
    if (!linkedTask) return false;
    const linkedFields = (linkedTask.fields as Record<string, string> | null) ?? {};
    const fieldVal = linkedFields[trigger.linkedColId] ?? '';
    return trigger.linkedOp === 'contains'
      ? fieldVal.toLowerCase().includes((trigger.linkedValue ?? '').toLowerCase())
      : fieldVal === (trigger.linkedValue ?? '');
  }

  // status-changes-to / dropdown-changes-to / text-contains
  type Cond = {
    colId: string; value: string; op: 'eq' | 'contains';
    linkedColId?: string; linkedValue?: string; linkedOp?: 'eq' | 'contains';
  };
  const scope = trigger.scope ?? 'all';

  const evalCond = async (c: Cond): Promise<boolean> => {
    if (c.linkedColId !== undefined) {
      // Link-column extra condition: look up the linked task's field value
      const linkedTaskId = fields[c.colId];
      if (!linkedTaskId) return false;
      const [linkedTask] = await database.select({ fields: taskTable.fields })
        .from(taskTable).where(eq(taskTable.id, linkedTaskId));
      if (!linkedTask) return false;
      const linkedFields = (linkedTask.fields as Record<string, string> | null) ?? {};
      const fieldVal = linkedFields[c.linkedColId] ?? '';
      return (c.linkedOp ?? 'eq') === 'contains'
        ? fieldVal.toLowerCase().includes((c.linkedValue ?? '').toLowerCase())
        : fieldVal === (c.linkedValue ?? '');
    }
    const fieldVal = fields[c.colId] ?? '';
    return c.op === 'contains'
      ? fieldVal.toLowerCase().includes(c.value.toLowerCase())
      : fieldVal === c.value;
  };

  const primary: Cond = {
    colId: trigger.colId,
    value: trigger.value,
    op: trigger.type === 'text-contains' ? 'contains' : 'eq',
  };

  const extras = (trigger.extraConditions ?? []).map(c => ({
    colId: c.colId,
    value: c.value,
    op: (c.op ?? 'eq') as 'eq' | 'contains',
    // Default connector falls back to scope for automations saved before per-condition connectors
    connector: (c.connector ?? (scope === 'any' ? 'or' : 'and')) as 'and' | 'or',
    linkedColId: c.linkedColId,
    linkedValue: c.linkedValue,
    linkedOp: c.linkedOp,
  }));

  if (extras.length === 0) return evalCond(primary);

  // Build groups using standard boolean precedence: AND binds tighter than OR.
  // 'and'-connected conditions stay in the same group (all must match within the group).
  // 'or' starts a new group.  At least one group must match overall.
  //
  // Note: scope only controls the DEFAULT connector added to new conditions in the UI
  // (scope='all' → 'and' default, scope='any' → 'or' default).  The actual evaluation
  // is driven entirely by the per-condition connectors — groups.every would silently
  // turn every 'or' into 'and' when scope='all', which is wrong.
  const groups: Cond[][] = [[primary]];
  for (const extra of extras) {
    if (extra.connector === 'or') {
      groups.push([extra]);
    } else {
      groups[groups.length - 1]!.push(extra);
    }
  }

  for (const group of groups) {
    const results = await Promise.all(group.map(evalCond));
    if (results.every(Boolean)) return true;
  }
  return false;
}

/**
 * When task `changedTaskId`'s own fields are updated, re-evaluate link-changes
 * automations on any task (across any board) that links TO this task via a link
 * column.  This makes link-changes triggers reactive to changes in the linked
 * task's data, not just to the moment the link is first established.
 */
async function runAutomationsForLinksToTask(changedTaskId: string) {
  const allAutos = await database.select()
    .from(automationTable)
    .where(eq(automationTable.enabled, 1));
  const linkAutos = allAutos.filter(a => a.trigger.type === 'link-changes');
  if (!linkAutos.length) return;

  const boardsToUpdate = new Set<string>();

  for (const auto of linkAutos) {
    const linkColId = auto.trigger.colId;
    // Find tasks on this automation's board whose link column points to changedTaskId.
    // PostgreSQL JSONB ->> operator extracts the field value as text.
    const linkingTasks = await database
      .select({ id: taskTable.id, groupId: taskTable.groupId, fields: taskTable.fields })
      .from(taskTable)
      .innerJoin(groupTable, eq(taskTable.groupId, groupTable.id))
      .where(
        and(
          eq(groupTable.boardId, auto.boardId),
          sql`${taskTable.fields} ->> ${linkColId} = ${changedTaskId}`,
        ),
      );

    for (const task of linkingTasks) {
      const fields = (task.fields as Record<string, string>) ?? {};
      if (Object.values(fields).includes('__revisit__')) continue;
      if (!(await evaluateTrigger(auto.trigger, fields))) continue;
      for (const action of auto.actions) {
        await executeAction(action, task.id, task.groupId, auto.boardId, fields);
        boardsToUpdate.add(auto.boardId);
      }
    }
  }

  for (const boardId of boardsToUpdate) broadcastBoardUpdate(boardId);
}

async function runTaskAutomations(
  taskId: string,
  groupId: string,
  fields: Record<string, string>,
) {
  // While a task has the Revisit status, all automations are suspended.
  if (Object.values(fields).includes('__revisit__')) return;

  const [group] = await database.select({ boardId: groupTable.boardId })
    .from(groupTable).where(eq(groupTable.id, groupId));
  if (!group) return;

  const automations = await database.select()
    .from(automationTable)
    .where(and(eq(automationTable.boardId, group.boardId), eq(automationTable.enabled, 1)));

  let fired = false;
  for (const auto of automations) {
    if (!(await evaluateTrigger(auto.trigger, fields))) continue;
    for (const action of auto.actions) {
      await executeAction(action, taskId, groupId, group.boardId, fields);
      fired = true;
    }
  }

  // Broadcast so clients pick up anything the automation wrote (e.g. a new subitem)
  if (fired) broadcastBoardUpdate(group.boardId);
}

/**
 * Like runTaskAutomations, but called when a SUBITEM's fields change.
 * For scope='all' triggers ("all [sub_column] changes to X"), verifies that
 * every subitem on the task satisfies the condition before firing — not just
 * the one that was just updated.  For scope='any' (the default when unset),
 * fires as soon as the changed subitem matches (same as before).
 */
async function runSubitemTriggeredAutomations(
  taskId: string,
  groupId: string,
  changedSubitemFields: Record<string, string>,
) {
  // Suspend automations if the parent task has the Revisit status.
  const [parentTask] = await database.select({ fields: taskTable.fields })
    .from(taskTable).where(eq(taskTable.id, taskId));
  if (parentTask && Object.values((parentTask.fields as Record<string, string>) ?? {}).includes('__revisit__')) return;

  const [group] = await database.select({ boardId: groupTable.boardId })
    .from(groupTable).where(eq(groupTable.id, groupId));
  if (!group) return;

  const automations = await database.select()
    .from(automationTable)
    .where(and(eq(automationTable.boardId, group.boardId), eq(automationTable.enabled, 1)));

  let fired = false;
  for (const auto of automations) {
    const scope = auto.trigger.scope ?? 'all';

    let shouldFire: boolean;
    if (scope === 'all') {
      // "All subitems" mode: only fire when every subitem of the task satisfies the condition.
      const allSubitems = await database
        .select({ fields: subitemTable.fields })
        .from(subitemTable)
        .where(eq(subitemTable.taskId, taskId));
      if (allSubitems.length === 0) {
        shouldFire = false;
      } else {
        const results = await Promise.all(
          allSubitems.map(s =>
            evaluateTrigger(auto.trigger, (s.fields as Record<string, string>) ?? {}),
          ),
        );
        shouldFire = results.every(Boolean);
      }
    } else {
      // "Any subitem" mode: fire if the changed subitem satisfies the condition.
      shouldFire = await evaluateTrigger(auto.trigger, changedSubitemFields);
    }

    if (!shouldFire) continue;
    for (const action of auto.actions) {
      await executeAction(action, taskId, groupId, group.boardId, changedSubitemFields);
      fired = true;
    }
  }

  if (fired) broadcastBoardUpdate(group.boardId);
}

// Per-board cooldown: prevents concurrent page-load scans from running
// automations twice when multiple users open the same board simultaneously.
const automationScanCooldown = new Map<string, number>();
const SCAN_COOLDOWN_MS = 10_000; // 10 seconds

/**
 * Scan every task in a board and run any automation whose trigger condition
 * currently matches — called on page load to catch up on pending actions.
 */
async function runBoardAutomations(boardId: string) {
  const now = Date.now();
  const lastRun = automationScanCooldown.get(boardId) ?? 0;
  if (now - lastRun < SCAN_COOLDOWN_MS) return;
  automationScanCooldown.set(boardId, now);
  const automations = await database.select()
    .from(automationTable)
    .where(and(eq(automationTable.boardId, boardId), eq(automationTable.enabled, 1)));

  if (!automations.length) return;

  const taskRows = await database
    .select({ id: taskTable.id, groupId: taskTable.groupId, fields: taskTable.fields })
    .from(taskTable)
    .innerJoin(groupTable, eq(taskTable.groupId, groupTable.id))
    .where(eq(groupTable.boardId, boardId));

  let fired = false;
  for (const task of taskRows) {
    const fields = (task.fields as Record<string, string>) ?? {};
    // Skip tasks with Revisit status — all automations suspended until manually cleared.
    if (Object.values(fields).includes('__revisit__')) continue;
    for (const auto of automations) {
      if (!(await evaluateTrigger(auto.trigger, fields))) continue;
      for (const action of auto.actions) {
        // For state-based triggers (status / dropdown / text-contains), only run
        // move-to-group and copy-to on the page-load scan (positional catch-up).
        // Skip create-* AND set-status / set-dropdown — those should only fire on
        // the specific field-change event that triggered the automation.
        // Without this guard, every board load re-applies the status override,
        // overwriting whatever value the user intentionally set since last load.
        //
        // For link-changes triggers the condition is event-like and already specific,
        // so all action types ARE expected to run on load as a catch-up mechanism.
        if (
          auto.trigger.type !== 'link-changes' &&
          auto.trigger.type !== 'date-is' &&
          (action.type === 'create-subitem' ||
           action.type === 'create-task' ||
           action.type === 'set-status' ||
           action.type === 'set-dropdown' ||
           action.type === 'set-notes')
        ) continue;
        await executeAction(action, task.id, task.groupId, boardId, fields);
        fired = true;
      }
    }
  }

  if (fired) broadcastBoardUpdate(boardId);
}

async function executeAction(
  action: AutomationAction,
  taskId: string,
  groupId: string,
  boardId: string,
  fields: Record<string, string>,
) {
  switch (action.type) {
    case 'set-status':
    case 'set-dropdown': {
      if (!action.colId || action.value === undefined) break;
      // Always fetch the task's own current fields from the DB rather than using
      // the `fields` parameter — which may be a subitem's fields when the
      // automation was triggered by a subitem change. Spreading subitem fields
      // onto the task would wipe every task field that wasn't also on the subitem.
      const [currentTask] = await database.select({ fields: taskTable.fields })
        .from(taskTable).where(eq(taskTable.id, taskId));
      if (!currentTask) break;
      const taskFields = (currentTask.fields as Record<string, string>) ?? {};
      if (taskFields[action.colId] === action.value) break; // already the target value — skip
      // Revisit is a protected status — automations cannot set it or change it
      if (action.type === 'set-status' && (action.value === '__revisit__' || taskFields[action.colId] === '__revisit__')) break;
      const newFields = { ...taskFields, [action.colId]: action.value };
      await database.update(taskTable)
        .set({ fields: newFields, updatedAt: new Date() })
        .where(eq(taskTable.id, taskId));
      break;
    }
    case 'set-notes': {
      if (!action.colId) break;
      const [currentTask] = await database.select({ fields: taskTable.fields })
        .from(taskTable).where(eq(taskTable.id, taskId));
      if (!currentTask) break;
      const taskFields = (currentTask.fields as Record<string, string>) ?? {};
      const newFields = { ...taskFields, [action.colId]: action.value ?? '' };
      await database.update(taskTable)
        .set({ fields: newFields, updatedAt: new Date() })
        .where(eq(taskTable.id, taskId));
      break;
    }
    case 'move-to-group': {
      if (!action.groupId) break;
      if (groupId === action.groupId) break; // already in the target group — skip
      await database.update(taskTable)
        .set({ groupId: action.groupId, expanded: 0, updatedAt: new Date() })
        .where(eq(taskTable.id, taskId));
      break;
    }
    case 'copy-to': {
      if (!action.groupId) break;
      const [source] = await database.select().from(taskTable).where(eq(taskTable.id, taskId));
      if (!source) break;
      // Skip if a task with the same name already exists in the target group
      const [dup] = await database.select({ id: taskTable.id })
        .from(taskTable)
        .where(and(
          eq(taskTable.groupId, action.groupId),
          sql`lower(${taskTable.name}) = lower(${source.name})`,
        ));
      if (dup) break;
      await database.insert(taskTable).values({
        id: nanoid(),
        groupId: action.groupId,
        name: source.name,
        fields: source.fields,
        position: Date.now(),
      });
      // Broadcast to the destination board if it differs from the source
      const destBoardId = await boardIdFromGroup(action.groupId);
      if (destBoardId && destBoardId !== boardId) broadcastBoardUpdate(destBoardId);
      break;
    }
    case 'create-subitem': {
      const subitemName = action.name ?? '';
      // Skip if a subitem with the exact same name already exists on this task
      const [existing] = await database.select({ id: subitemTable.id })
        .from(subitemTable)
        .where(and(
          eq(subitemTable.taskId, taskId),
          sql`lower(${subitemTable.name}) = lower(${subitemName})`,
        ));
      if (existing) break;
      await database.insert(subitemTable).values({
        id: nanoid(),
        taskId,
        name: subitemName,
        position: Date.now(),
      });
      break;
    }
    case 'create-task': {
      const taskName = action.name ?? '';
      // Skip if a task with the exact same name already exists in this group
      const [existingTask] = await database.select({ id: taskTable.id })
        .from(taskTable)
        .where(and(
          eq(taskTable.groupId, groupId),
          sql`lower(${taskTable.name}) = lower(${taskName})`,
        ));
      if (existingTask) break;
      await database.insert(taskTable).values({
        id: nanoid(),
        groupId,
        name: taskName,
        position: Date.now(),
      });
      break;
    }
  }
}

// ── Merge duplicate into existing ────────────────────────────────────────────

/**
 * When a user creates a task whose name already exists and chooses to edit the
 * existing one, this procedure atomically:
 *   1. Moves the existing task to the new task's group
 *   2. Sets the existing task's first status column to the reserved '__revisit__' key
 *   3. Deletes the new empty task
 * While '__revisit__' is set, ALL automations are blocked on that task.
 * They resume the moment the user manually changes the main status.
 */
export const mergeIntoExisting = workspaceScoped
  .input(z.object({ newTaskId: z.string(), existingTaskId: z.string() }))
  .handler(async ({ input }) => {
    const boardId = await boardIdFromTask(input.existingTaskId);
    if (!boardId) throw new ORPCError('NOT_FOUND', { message: 'Task not found.' });

    // Move the existing task to the new task's group
    const [newTask] = await database.select({ groupId: taskTable.groupId })
      .from(taskTable).where(eq(taskTable.id, input.newTaskId));
    if (newTask) {
      await database.update(taskTable)
        .set({ groupId: newTask.groupId, updatedAt: new Date() })
        .where(eq(taskTable.id, input.existingTaskId));
    }

    // Set Revisit on the first status column
    const [statusCol] = await database.select({ id: columnTable.id })
      .from(columnTable)
      .where(and(
        eq(columnTable.boardId, boardId),
        eq(columnTable.type, 'status'),
      ))
      .orderBy(asc(columnTable.position));

    if (statusCol) {
      const [existing] = await database.select({ fields: taskTable.fields })
        .from(taskTable).where(eq(taskTable.id, input.existingTaskId));
      if (existing) {
        const fields = (existing.fields as Record<string, string>) ?? {};
        await database.update(taskTable)
          .set({ fields: { ...fields, [statusCol.id]: '__revisit__' }, updatedAt: new Date() })
          .where(eq(taskTable.id, input.existingTaskId));
      }
    }

    // Delete the new empty task (ignore if already gone)
    await database.delete(taskTable).where(eq(taskTable.id, input.newTaskId));

    broadcastBoardUpdate(boardId);
    return { success: true };
  });

// ── On-load automation check ──────────────────────────────────────────────────

/**
 * Called once per board page load. Scans all tasks against all enabled
 * automations and fires any whose conditions are currently satisfied.
 * Runs in the background — the client picks up results via the SSE broadcast.
 */
export const checkBoardAutomations = workspaceScoped
  .input(z.object({ boardId: z.string() }))
  .handler(async ({ input, context }) => {
    const [board] = await database.select({ id: boardTable.id })
      .from(boardTable)
      .where(and(eq(boardTable.id, input.boardId), eq(boardTable.workspaceId, context.workspace.id)));
    if (!board) throw new ORPCError('NOT_FOUND', { message: 'Board not found.' });
    // Fire and forget — result arrives via broadcastBoardUpdate → SSE → invalidateQueries
    runBoardAutomations(input.boardId).catch(() => {});
    return { success: true };
  });
