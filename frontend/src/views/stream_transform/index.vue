<template>
  <section class="page" data-module="stream_transform">
    <header class="page-head">
      <div><h2>流式转换</h2><p class="page-desc">维护流式数据转换算子的定义、并行配置和背压控制，串联实时数据处理链路。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记转换算子</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>
    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>
    <section class="batch-panel" data-testid="batch-panel">
      <header class="batch-head">
        <div>
          <h3>订单转换批次</h3>
          <p class="page-desc">新订单先落批次再逐行校验：金额、时间、用户字段不合法或时间缺时区的行进入隔离区，原始值保留可追溯。</p>
        </div>
        <div class="batch-publish">
          <label class="create-field"><span>窗口编号</span><input v-model="publishWindow" data-testid="publish-window" /></label>
          <label class="create-field batch-orders"><span>订单列表（JSON）</span><textarea v-model="publishOrders" data-testid="publish-orders" rows="4"></textarea></label>
          <button class="btn primary" type="button" data-testid="publish-batch" :disabled="publishing" @click="publishBatch">
            {{ publishing ? '发布中…' : '发布订单批次' }}
          </button>
        </div>
      </header>
      <p v-if="batchMessage" class="batch-message" :class="{ 'error-text': batchFailed }" data-testid="batch-message">{{ batchMessage }}</p>

      <table class="data-table batch-table">
        <thead>
          <tr><th>批次号</th><th>窗口编号</th><th>状态</th><th>接收</th><th>已发布</th><th>已隔离</th><th>待确认</th><th>已发布金额</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="batch in batches" :key="batch.batch_no" :data-testid="`batch-${batch.batch_no}`" :data-status="batch.status">
            <td>{{ batch.batch_no }}</td>
            <td>{{ batch.window_no }}</td>
            <td>{{ batch.status }}</td>
            <td>{{ batch.received_count }}</td>
            <td>{{ batch.published_count }}</td>
            <td>{{ batch.quarantined_count }}</td>
            <td>{{ batch.pending_count }}</td>
            <td>{{ batch.published_amount }}</td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`inspect-${batch.batch_no}`" @click="inspectBatch(batch.batch_no)">核对清单</button>
              <button class="link" type="button" :data-testid="`retry-${batch.batch_no}`" :disabled="!batch.pending_count" @click="retryBatch(batch.batch_no)">重试续跑</button>
            </td>
          </tr>
          <tr v-if="!batches.length"><td colspan="9" class="empty-state">暂无转换批次，可先发布一批订单</td></tr>
        </tbody>
      </table>

      <div v-if="activeBatch" class="batch-detail" data-testid="batch-detail">
        <h4>批次 {{ activeBatch.batch_no }} 核对清单</h4>
        <table class="data-table">
          <thead>
            <tr><th>源订单编号</th><th>状态</th><th>核对状态</th><th>金额</th><th>事件时间</th><th>用户</th><th>原始金额</th><th>原始时间</th><th>原始用户</th><th>隔离原因</th><th>操作</th></tr>
          </thead>
          <tbody>
            <tr v-for="record in activeBatch.records" :key="record.source_order_id || record.row_no" :data-testid="`record-${record.source_order_id}`" :data-status="record.status">
              <td>{{ record.source_order_id || '（缺源单号）' }}</td>
              <td>{{ record.status }}</td>
              <td>{{ record['核对状态'] }}</td>
              <td>{{ record.amount ?? '—' }}</td>
              <td>{{ record.event_time ?? '—' }}</td>
              <td>{{ record.user_id ?? '—' }}</td>
              <td>{{ record.raw_amount ?? '—' }}</td>
              <td>{{ record.raw_event_time ?? '—' }}</td>
              <td>{{ record.raw_user_id ?? '—' }}</td>
              <td>{{ record.reject_reason || '—' }}</td>
              <td class="row-actions">
                <button class="link" type="button" :disabled="record.booked || record.status === '已发布'" :data-testid="`correct-${record.source_order_id}`" @click="openCorrection(record)">修正</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
    <div v-if="correcting" class="modal-backdrop" data-testid="correct-modal">
      <form class="modal" @submit.prevent="submitCorrection">
        <h3>修正隔离订单 {{ correcting.source_order_id }}</h3>
        <p class="page-desc">原始值：金额 {{ correcting.raw_amount ?? '—' }}，时间 {{ correcting.raw_event_time ?? '—' }}，用户 {{ correcting.raw_user_id ?? '—' }}</p>
        <label class="create-field"><span>金额</span><input v-model="correctionValues.amount" data-testid="correct-amount" placeholder="留空表示不修改" /></label>
        <label class="create-field"><span>事件时间（需带时区）</span><input v-model="correctionValues.event_time" data-testid="correct-event-time" placeholder="2026-11-16T09:00:00Z" /></label>
        <label class="create-field"><span>用户</span><input v-model="correctionValues.user_id" data-testid="correct-user" placeholder="留空表示不修改" /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="correcting = null">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-correction">提交修正</button>
        </div>
      </form>
    </div>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记转换算子</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot"><span>{{ graph.nodes.length }} 个节点</span></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/stream_transform'
const columns = ["算子编号", "输入流", "输出流", "转换类型", "并行实例", "背压阈值", "状态大小", "算子耗时"]
const actions = ["部署算子", "降级算子", "停止算子"]
const statuses = ["未部署", "运行中", "已降级", "已停止"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','待校验':'#fef3c7','已启用':'#d1fae5','已停用':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<number,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)
function layout(nodes: any[]) {
  const w = 800; const h = Math.max(400, nodes.length * 80)
  svgW.value = w; svgH.value = h
  const pos: Record<number,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function startDrag(e: MouseEvent, node: any) { dragNode = node; const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }; const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }; document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp) }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); graph.nodes = data.nodes || []; graph.edges = data.edges || []; layout(graph.nodes) } catch(e) { errorMessage.value = '加载失败' }
}
async function runAction(action: string, node: any) {
  try { await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) }); await reload() } catch(e) { errorMessage.value = '操作失败' }
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }

// ---- 订单转换批次：发布、隔离修正、失败续跑 ----
const BATCH_ENDPOINT = '/api/order_transform'
const publishWindow = ref('WIN-TUMBLE-60s')
const publishOrders = ref('[\n  {"源订单编号": "ORD-011", "金额": 128.5, "时间": "2026-11-16T08:11:00Z", "用户ID": "U1001", "渠道": "app"}\n]')
const publishing = ref(false)
const batchMessage = ref('')
const batchFailed = ref(false)
const batches = ref<any[]>([])
const activeBatch = ref<any>(null)
const correcting = ref<any>(null)
const correctionValues = ref<Record<string, string>>({ amount: '', event_time: '', user_id: '' })

async function loadBatches() {
  try {
    const res = await request(`${BATCH_ENDPOINT}/batches`)
    const payload = await res.json()
    batches.value = payload.items ?? []
    if (activeBatch.value) {
      const fresh = batches.value.find((batch) => batch.batch_no === activeBatch.value.batch_no)
      if (fresh) await inspectBatch(fresh.batch_no)
    }
  } catch (e) {
    errorMessage.value = '转换批次加载失败'
  }
}

function showBatchResult(payload: any) {
  batchMessage.value = payload.message || ''
  batchFailed.value = !payload.ok
}

async function publishBatch() {
  publishing.value = true
  batchMessage.value = ''
  batchFailed.value = false
  try {
    let orders: unknown
    try {
      orders = JSON.parse(publishOrders.value)
    } catch {
      throw new Error('订单列表不是合法 JSON，请检查格式')
    }
    const res = await request(`${BATCH_ENDPOINT}/batches`, {
      method: 'POST',
      body: JSON.stringify({ window_no: publishWindow.value, orders }),
    })
    const payload = await res.json()
    showBatchResult(payload)
    if (payload.entry?.batch_no) await inspectBatch(payload.entry.batch_no)
    await loadBatches()
  } catch (e) {
    batchFailed.value = true
    batchMessage.value = e instanceof Error ? e.message : '批次发布失败'
  } finally {
    publishing.value = false
  }
}

async function inspectBatch(batchNo: string) {
  try {
    const res = await request(`${BATCH_ENDPOINT}/batches/${batchNo}`)
    if (!res.ok) throw new Error('批次读取失败')
    activeBatch.value = await res.json()
  } catch (e) {
    activeBatch.value = null
    errorMessage.value = e instanceof Error ? e.message : '批次读取失败'
  }
}

async function retryBatch(batchNo: string) {
  try {
    const res = await request(`${BATCH_ENDPOINT}/batches/${batchNo}/retry`, { method: 'POST', body: '{}' })
    const payload = await res.json()
    showBatchResult(payload)
    await inspectBatch(batchNo)
    await loadBatches()
  } catch (e) {
    batchFailed.value = true
    batchMessage.value = e instanceof Error ? e.message : '批次重试失败'
  }
}

function openCorrection(record: any) {
  correcting.value = record
  correctionValues.value = { amount: '', event_time: '', user_id: '' }
}

async function submitCorrection() {
  if (!correcting.value || !activeBatch.value) return
  const body: Record<string, string> = {}
  for (const [key, value] of Object.entries(correctionValues.value)) {
    if (value.trim()) body[key] = value.trim()
  }
  try {
    const res = await request(
      `${BATCH_ENDPOINT}/batches/${activeBatch.value.batch_no}/records/${encodeURIComponent(correcting.value.source_order_id)}`,
      { method: 'POST', body: JSON.stringify(body) },
    )
    const payload = await res.json()
    showBatchResult(payload)
    correcting.value = null
    await inspectBatch(activeBatch.value.batch_no)
    await loadBatches()
  } catch (e) {
    batchFailed.value = true
    batchMessage.value = e instanceof Error ? e.message : '修正失败'
  }
}

onMounted(reload)
onMounted(loadBatches)
</script>
