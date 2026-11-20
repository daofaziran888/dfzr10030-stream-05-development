<template>
  <section class="page" data-module="window_agg">
    <header class="page-head">
      <div>
        <h2>窗口聚合管理</h2>
        <p class="page-desc">维护流式窗口聚合的定义、迟到数据处理和状态过期策略。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记时间窗口</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出窗口聚合清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <section class="batch-panel" data-testid="window-settlement">
      <header class="batch-head">
        <div>
          <h3>窗口实际入账</h3>
          <p class="page-desc">窗口只对已发布的转换批次记录出数；隔离与待确认的订单单独列为未到账范围，实际入账后回写转换核对清单。</p>
        </div>
        <div class="batch-publish">
          <label class="create-field"><span>窗口编号</span><input v-model="windowNo" data-testid="settle-window" @change="loadSettlement" /></label>
          <button class="btn" type="button" data-testid="load-settlement" @click="loadSettlement">刷新出数</button>
          <button class="btn primary" type="button" data-testid="settle-window-action" :disabled="settling" @click="settleWindow">
            {{ settling ? '入账中…' : '确认实际入账' }}
          </button>
        </div>
      </header>
      <p v-if="settleMessage" class="batch-message" :class="{ 'error-text': settleFailed }" data-testid="settle-message">{{ settleMessage }}</p>
      <div v-if="settlement" class="stat-row">
        <article class="stat-card"><span class="stat-label">已发布笔数</span><strong class="stat-value" data-testid="published-count">{{ settlement.published.count }}</strong></article>
        <article class="stat-card"><span class="stat-label">已发布金额</span><strong class="stat-value" data-testid="published-amount">{{ settlement.published.amount }}</strong></article>
        <article class="stat-card"><span class="stat-label">已入账笔数</span><strong class="stat-value" data-testid="settled-count">{{ settlement.settled.count }}</strong></article>
        <article class="stat-card"><span class="stat-label">未到账笔数</span><strong class="stat-value" data-testid="pending-count">{{ settlement.pending.count }}</strong></article>
      </div>
      <p v-if="settlement" class="page-desc" data-testid="pending-range">
        未到账范围：{{ settlement.pending.range ? `${settlement.pending.range.from} ~ ${settlement.pending.range.to}` : '暂无待确认记录' }}
        <span v-if="settlement.pending.order_ids.length">（{{ settlement.pending.order_ids.join('、') }}）</span>
        · 已发布但未入账：{{ settlement.pending.unbooked_published }}
      </p>
      <table v-if="settlement" class="data-table">
        <thead>
          <tr><th>源订单编号</th><th>批次号</th><th>状态</th><th>核对状态</th><th>金额</th><th>事件时间</th><th>入账时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="record in settlement.checklist" :key="record.source_order_id || record.row_no" :data-testid="`checklist-${record.source_order_id}`" :data-status="record['核对状态']">
            <td>{{ record.source_order_id || '（缺源单号）' }}</td>
            <td>{{ record.batch_no }}</td>
            <td>{{ record.status }}</td>
            <td>{{ record['核对状态'] }}</td>
            <td>{{ record.amount ?? '—' }}</td>
            <td>{{ record.event_time ?? record.raw_event_time ?? '—' }}</td>
            <td>{{ record.booked_at ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无窗口聚合数据，可先登记时间窗口</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记时间窗口</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条窗口聚合记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/window_agg'
const columns = ["窗口编号", "聚合指标", "窗口类型", "窗口长度", "滑动步长", "迟到阈值", "发射频率", "状态过期"]
const actions = ["启效窗口", "暂停窗口", "废弃窗口"]
const statuses = ["草稿", "生效中", "已暂停", "已废弃"]
const metricNames = ["窗口总数", "生效中窗口", "已暂停窗口"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'window_agg.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('窗口聚合动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '窗口聚合操作失败'
  }
}

// ---- 窗口实际入账：只对已发布批次出数，入账后回写核对清单 ----
const SETTLE_ENDPOINT = '/api/order_transform'
const windowNo = ref('WIN-TUMBLE-60s')
const settlement = ref<any>(null)
const settleMessage = ref('')
const settleFailed = ref(false)
const settling = ref(false)

async function loadSettlement() {
  settleMessage.value = ''
  settleFailed.value = false
  try {
    const res = await request(`${SETTLE_ENDPOINT}/windows/${encodeURIComponent(windowNo.value)}`)
    if (!res.ok) throw new Error('窗口出数读取失败')
    settlement.value = await res.json()
  } catch (error) {
    settlement.value = null
    settleMessage.value = error instanceof Error ? error.message : '窗口出数读取失败'
    settleFailed.value = true
  }
}

async function settleWindow() {
  settling.value = true
  try {
    const res = await request(`${SETTLE_ENDPOINT}/windows/${encodeURIComponent(windowNo.value)}/settle`, { method: 'POST', body: '{}' })
    const payload = await res.json()
    settleMessage.value = payload.message || ''
    settleFailed.value = !payload.ok
    if (payload.entry) settlement.value = payload.entry
    else await loadSettlement()
  } catch (error) {
    settleMessage.value = error instanceof Error ? error.message : '入账失败'
    settleFailed.value = true
  } finally {
    settling.value = false
  }
}

async function reload() {  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('时间窗口列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '窗口聚合列表读取失败'
  }
}

onMounted(reload)
onMounted(loadSettlement)
</script>
