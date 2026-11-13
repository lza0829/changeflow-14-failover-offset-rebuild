<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const streams = ref([])
const streamId = ref(1)
const tasks = ref([])
const selected = ref(null)
const badCursor = ref(0)
const phantomSeq = ref(99)
const err = ref('')
const busy = ref(false)

async function refresh() {
  streams.value = await getJSON('/api/streams')
  tasks.value = await getJSON('/api/failover-rebuild/tasks')
  if (selected.value) {
    selected.value = tasks.value.find((t) => t.id === selected.value.id) || null
  }
}

async function startRebuild() {
  busy.value = true
  err.value = ''
  try {
    selected.value = await postJSON('/api/failover-rebuild/tasks', { stream_id: streamId.value })
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function confirmTask() {
  busy.value = true
  err.value = ''
  try {
    selected.value = await postJSON(`/api/failover-rebuild/tasks/${selected.value.id}/confirm`, {})
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function rejectTask() {
  busy.value = true
  err.value = ''
  try {
    selected.value = await postJSON(`/api/failover-rebuild/tasks/${selected.value.id}/reject`, {
      reason: '页面人工拒绝',
    })
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function tamper() {
  busy.value = true
  err.value = ''
  try {
    await postJSON('/api/failover-rebuild/demo/tamper-cursor', {
      stream_id: streamId.value,
      bad_cursor: Number(badCursor.value),
    })
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function injectConflict() {
  busy.value = true
  err.value = ''
  try {
    await postJSON('/api/failover-rebuild/demo/inject-sink-conflict', {
      stream_id: streamId.value,
      bad_cursor: Number(phantomSeq.value),
    })
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function clearDemo() {
  busy.value = true
  err.value = ''
  try {
    await postJSON('/api/failover-rebuild/demo/clear', { stream_id: streamId.value })
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

function statusTag(s) {
  if (s === 'confirmed') return 'ok'
  if (s === 'awaiting_confirmation') return 'warn'
  return 'danger'
}

function pickTask(t) {
  selected.value = t
}

onMounted(refresh)
</script>

<template>
  <div class="page">
    <h1>故障位点重建</h1>
    <p class="muted">
      apply_cursor 丢失/改坏后，以 Sink 与已应用账本为证据重建候选位点，人工确认后才切换。
      不是回拨重放，也不是行级校验/漂移门禁。可对照
      <router-link to="/ledger">变更账本</router-link>、
      <router-link to="/sink">Sink</router-link>、
      <router-link to="/cursors">游标</router-link> 页核验。
    </p>

    <div class="row-actions panel">
      <label>流
        <select v-model.number="streamId">
          <option v-for="s in streams" :key="s.id" :value="s.id">
            {{ s.id }} · {{ s.name }}（当前游标 {{ s.apply_cursor }}）
          </option>
        </select>
      </label>
      <button :disabled="busy" @click="startRebuild">{{ busy ? '扫描中…' : '发起重建（扫证据）' }}</button>
    </div>

    <div class="panel">
      <h3>演练 / 验收工具（故障注入，非主链）</h3>
      <div class="row-actions">
        <label>坏游标值 <input v-model="badCursor" type="number" style="width: 6rem" /></label>
        <button class="ghost" :disabled="busy" @click="tamper">改坏游标</button>
        <label>幻影 Sink seq <input v-model="phantomSeq" type="number" style="width: 6rem" /></label>
        <button class="ghost" :disabled="busy" @click="injectConflict">注入 Sink/账本矛盾</button>
        <button class="ghost" :disabled="busy" @click="clearDemo">清理注入数据</button>
      </div>
    </div>

    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel" v-if="selected">
      <h3>
        重建任务 #{{ selected.id }}
        <span class="tag" :class="statusTag(selected.status)">{{ selected.status }}</span>
        <span class="tag warn">gate: {{ selected.gate }}</span>
      </h3>
      <table>
        <tbody>
          <tr><th>流</th><td>{{ selected.stream_id }}</td></tr>
          <tr><th>候选位点</th><td><b>{{ selected.candidate_cursor }}</b>（adoptable: {{ String(selected.adoptable) }}）</td></tr>
          <tr><th>切换前游标</th><td>{{ selected.cursor_before }}</td></tr>
          <tr><th>切换后游标</th><td>{{ selected.cursor_after }}</td></tr>
          <tr><th>创建 / 决策时间</th><td>{{ selected.created_at }} / {{ selected.decided_at || '—' }}</td></tr>
        </tbody>
      </table>

      <h4>扫描规模</h4>
      <table>
        <thead><tr><th>证据源</th><th>扫描行数</th></tr></thead>
        <tbody>
          <tr v-for="(v, k) in selected.scan_counts" :key="k">
            <td>{{ k }}</td><td>{{ v }}</td>
          </tr>
        </tbody>
      </table>

      <h4>证据摘要</h4>
      <table>
        <tbody>
          <tr><th>applied_seqs</th><td class="mono">{{ selected.evidence.applied_seqs.join(', ') || '（空）' }}</td></tr>
          <tr><th>sink last_seq</th><td class="mono">{{ selected.evidence.sink_last_seqs.join(', ') || '（空）' }}</td></tr>
          <tr><th>quarantine seq</th><td class="mono">{{ selected.evidence.quarantine_seqs.join(', ') || '（空）' }}</td></tr>
          <tr><th>计数</th><td class="muted">applied {{ selected.evidence.applied_count }} · sink {{ selected.evidence.sink_record_count }} · 隔离 {{ selected.evidence.quarantine_count }} · ledger {{ selected.evidence.ledger_applied_count }}</td></tr>
        </tbody>
      </table>

      <h4>计算依据</h4>
      <ul class="mono" style="font-size:0.85rem">
        <li v-for="(b, i) in selected.basis" :key="i">{{ b }}</li>
      </ul>

      <div v-if="selected.reasons.length">
        <h4>拒绝原因</h4>
        <ul style="color: var(--danger); font-size: 0.9rem">
          <li v-for="(r, i) in selected.reasons" :key="i">{{ r }}</li>
        </ul>
      </div>

      <h4>审计</h4>
      <pre class="mono">{{ JSON.stringify(selected.audit, null, 2) }}</pre>

      <div class="row-actions" v-if="selected.status === 'awaiting_confirmation'">
        <button :disabled="busy" @click="confirmTask">确认采纳并切换游标</button>
        <button class="ghost" :disabled="busy" @click="rejectTask">拒绝（游标不动）</button>
      </div>
      <p v-else class="muted">任务已终态，不可再决策。</p>
    </div>

    <div class="panel">
      <h3>重建任务记录（持久化）</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>流</th><th>状态</th><th>gate</th><th>候选</th><th>采纳</th><th>游标前→后</th><th>时间</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-for="t in tasks" :key="t.id">
            <td>{{ t.id }}</td>
            <td>{{ t.stream_id }}</td>
            <td><span class="tag" :class="statusTag(t.status)">{{ t.status }}</span></td>
            <td>{{ t.gate }}</td>
            <td>{{ t.candidate_cursor }}</td>
            <td>{{ String(t.adoptable) }}</td>
            <td>{{ t.cursor_before }} → {{ t.cursor_after }}</td>
            <td>{{ t.created_at }}</td>
            <td><a href="#" @click.prevent="pickTask(t)">查看证据</a></td>
          </tr>
          <tr v-if="!tasks.length"><td colspan="9" class="muted">暂无重建任务</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
